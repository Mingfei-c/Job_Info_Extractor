"""
Job Fetch Service - Adzuna API
Fetch jobs from Adzuna API and store in PostgreSQL database
Includes complete rate limiting control
"""

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text, create_engine, func
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")  # Must be configured in .env
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Adzuna API configuration
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
ADZUNA_COUNTRY = os.getenv("ADZUNA_COUNTRY", "us")  # Default: US


# ==================== Rate Limiting Configuration ====================


class RateLimits:
    """Rate limiting configuration"""

    # Sliding window limit
    WINDOW_SECONDS = 70  # 70-second window
    WINDOW_MAX_REQUESTS = 25  # Max 25 requests per window

    # Natural time period limits (with buffer)
    DAILY_MAX = 240  # Daily max (buffer from 250)
    WEEKLY_MAX = 960  # Weekly max (buffer from 1000)
    MONTHLY_MAX = 2400  # Monthly max (buffer from 2500)

    # Jobs returned per request
    JOBS_PER_REQUEST = 20


# ==================== Database Models ====================


class Job(Base):
    """Job listing table"""

    __tablename__ = "adzuna_jobs"

    id = Column(Integer, primary_key=True, index=True)
    adzuna_id = Column(String(100), unique=True, index=True)  # Adzuna job ID
    title = Column(String(500), nullable=False)
    company_name = Column(String(255))
    category = Column(String(100))
    location = Column(String(255))
    salary_min = Column(Float)
    salary_max = Column(Float)
    description = Column(Text)
    redirect_url = Column(String(1000))
    created_date = Column(DateTime)  # Publication date on Adzuna
    fetched_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=True)
    is_scraped = Column(
        Boolean, default=False, index=True
    )  # Whether full description has been scraped


class ApiCallLog(Base):
    """API call log table - records timestamp of each call"""

    __tablename__ = "api_call_logs"

    id = Column(Integer, primary_key=True, index=True)
    call_time = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    endpoint = Column(String(255))
    page = Column(Integer)
    jobs_fetched = Column(Integer, default=0)
    status = Column(String(50))  # success, failed
    error_message = Column(Text)
    response_time_ms = Column(Integer)


# Create tables
Base.metadata.create_all(bind=engine)


# ==================== Rate Limiter ====================


class RateLimiter:
    """
    Rate limiter
    - Sliding window: max 25 requests per 70 seconds
    - Natural day/week/month limits
    """

    def __init__(self, db_session):
        self.db = db_session
        self.recent_calls = deque()  # Sliding window records

    def _get_utc_now(self) -> datetime:
        """Get current UTC time"""
        return datetime.now(timezone.utc)

    def _get_day_start(self, dt: datetime) -> datetime:
        """Get start of current day (UTC)"""
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_week_start(self, dt: datetime) -> datetime:
        """Get start of current week (UTC, Monday as start)"""
        day_start = self._get_day_start(dt)
        days_since_monday = dt.weekday()
        return day_start.replace(day=dt.day - days_since_monday)

    def _get_month_start(self, dt: datetime) -> datetime:
        """Get start of current month (UTC)"""
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def get_calls_in_period(self, start_time: datetime) -> int:
        """Get number of successful calls after a given timestamp"""
        count = (
            self.db.query(func.count(ApiCallLog.id))
            .filter(ApiCallLog.call_time >= start_time, ApiCallLog.status == "success")
            .scalar()
        )
        return count or 0

    def get_remaining_quota(self) -> dict:
        """Get remaining quota"""
        now = self._get_utc_now()

        # Calculate usage for each time period
        daily_used = self.get_calls_in_period(self._get_day_start(now))
        weekly_used = self.get_calls_in_period(self._get_week_start(now))
        monthly_used = self.get_calls_in_period(self._get_month_start(now))

        return {
            "daily": {
                "used": daily_used,
                "limit": RateLimits.DAILY_MAX,
                "remaining": max(0, RateLimits.DAILY_MAX - daily_used),
            },
            "weekly": {
                "used": weekly_used,
                "limit": RateLimits.WEEKLY_MAX,
                "remaining": max(0, RateLimits.WEEKLY_MAX - weekly_used),
            },
            "monthly": {
                "used": monthly_used,
                "limit": RateLimits.MONTHLY_MAX,
                "remaining": max(0, RateLimits.MONTHLY_MAX - monthly_used),
            },
        }

    def can_make_request(self) -> tuple[bool, str]:
        """
        Check whether a request can be made
        Returns: (can_request, reason)
        """
        now = self._get_utc_now()

        # Remove expired sliding window records
        window_start = now.timestamp() - RateLimits.WINDOW_SECONDS
        while self.recent_calls and self.recent_calls[0] < window_start:
            self.recent_calls.popleft()

        # Check sliding window
        if len(self.recent_calls) >= RateLimits.WINDOW_MAX_REQUESTS:
            wait_time = self.recent_calls[0] + RateLimits.WINDOW_SECONDS - now.timestamp()
            return False, f"Sliding window limit: wait {wait_time:.1f} seconds"

        # Check daily/weekly/monthly limits
        quota = self.get_remaining_quota()

        if quota["daily"]["remaining"] <= 0:
            return False, "Daily limit reached (240 requests)"

        if quota["weekly"]["remaining"] <= 0:
            return False, "Weekly limit reached (960 requests)"

        if quota["monthly"]["remaining"] <= 0:
            return False, "Monthly limit reached (2400 requests)"

        return True, "OK"

    def record_call(self):
        """Record one call in the sliding window"""
        self.recent_calls.append(self._get_utc_now().timestamp())

    def wait_if_needed(self) -> bool:
        """
        Wait if necessary
        Returns: True if can proceed, False if daily/weekly/monthly limit reached
        """
        while True:
            can_request, reason = self.can_make_request()

            if can_request:
                return True

            # If daily/weekly/monthly limit, stop without waiting
            if "Daily limit" in reason or "Weekly limit" in reason or "Monthly limit" in reason:
                logger.warning(f"Stopping fetch: {reason}")
                return False

            # Sliding window limit, wait
            logger.info(f"Rate limit: {reason}")
            time.sleep(5)  # Check every 5 seconds


# ==================== Adzuna API Client ====================


class AdzunaAPI:
    """Adzuna API client"""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs"

    def __init__(self, app_id: str, app_key: str, country: str = "us"):
        self.app_id = app_id
        self.app_key = app_key
        self.country = country

    def search_jobs(
        self, page: int = 1, what: str = "", where: str = "", max_days_old: int | None = None
    ) -> dict:
        """
        Search for jobs

        Args:
            page: Page number (starting from 1)
            what: Search keywords
            where: Location
            max_days_old: Only fetch jobs posted within the last N days

        Returns:
            API response data
        """
        url = f"{self.BASE_URL}/{self.country}/search/{page}"

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": RateLimits.JOBS_PER_REQUEST,
            "sort_by": "date",
        }

        if what:
            params["what"] = what
        if where:
            params["where"] = where
        if max_days_old:
            params["max_days_old"] = max_days_old

        start_time = time.time()
        response = requests.get(url, params=params, timeout=30)
        response_time = int((time.time() - start_time) * 1000)

        response.raise_for_status()

        data = response.json()
        data["_response_time_ms"] = response_time

        return data


# ==================== Job Fetch Service ====================


class JobFetchService:
    """Job fetch and storage service"""

    def __init__(self):
        self.db = SessionLocal()
        self.rate_limiter = RateLimiter(self.db)
        self.api = AdzunaAPI(ADZUNA_APP_ID, ADZUNA_APP_KEY, ADZUNA_COUNTRY)

    def __del__(self):
        self.db.close()

    def fetch_all_available(
        self, what: str = "", where: str = "", max_days_old: int = 7, max_pages: int = 240
    ) -> dict:
        """
        Fetch as many jobs as possible in one run (without hitting rate limits)

        Args:
            what: Search keywords
            where: Location
            max_days_old: Only fetch jobs posted within the last N days
            max_pages: Maximum pages to fetch (default 240, one day's allowance)

        Returns:
            Fetch result statistics
        """
        logger.info("=" * 60)
        logger.info("Starting batch job fetch")
        logger.info(f"Search params: what='{what}', where='{where}', max_days_old={max_days_old}")

        # Check quota
        quota = self.rate_limiter.get_remaining_quota()
        logger.info(
            f"Current quota: daily={quota['daily']['remaining']}, "
            f"weekly={quota['weekly']['remaining']}, monthly={quota['monthly']['remaining']}"
        )

        # Calculate actual pages to fetch
        available_requests = min(
            quota["daily"]["remaining"],
            quota["weekly"]["remaining"],
            quota["monthly"]["remaining"],
            max_pages,
        )

        if available_requests <= 0:
            logger.warning("No quota available, stopping fetch")
            return {
                "status": "no_quota",
                "pages_fetched": 0,
                "jobs_fetched": 0,
                "jobs_new": 0,
                "jobs_updated": 0,
            }

        logger.info(f"Planning to fetch {available_requests} pages")

        # Start fetching
        total_jobs_fetched = 0
        jobs_new = 0
        jobs_updated = 0
        pages_fetched = 0

        for page in range(1, available_requests + 1):
            # Check and wait for rate limit
            if not self.rate_limiter.wait_if_needed():
                break

            # Make request
            result = self._fetch_page(page, what, where, max_days_old)

            if result is None:
                logger.error(f"Page {page} fetch failed, stopping")
                break

            pages_fetched += 1
            total_jobs_fetched += result["jobs_count"]
            jobs_new += result["new"]
            jobs_updated += result["updated"]

            # Record call
            self.rate_limiter.record_call()

            # Stop if no more jobs
            if result["jobs_count"] < RateLimits.JOBS_PER_REQUEST:
                logger.info("No more jobs available, stopping fetch")
                break

            # Progress log
            if page % 10 == 0:
                logger.info(
                    f"Progress: {page}/{available_requests} pages, {total_jobs_fetched} jobs fetched"
                )

        # Final statistics
        result = {
            "status": "success",
            "pages_fetched": pages_fetched,
            "jobs_fetched": total_jobs_fetched,
            "jobs_new": jobs_new,
            "jobs_updated": jobs_updated,
            "quota_remaining": self.rate_limiter.get_remaining_quota(),
        }

        logger.info("=" * 60)
        logger.info(
            f"Fetch complete: {pages_fetched} pages, {total_jobs_fetched} jobs, "
            f"{jobs_new} new, {jobs_updated} updated"
        )

        return result

    def _fetch_page(self, page: int, what: str, where: str, max_days_old: int) -> Optional[dict]:
        """
        Fetch and store a single page of jobs

        Returns:
            {"jobs_count": N, "new": N, "updated": N} or None on failure
        """
        log_entry = ApiCallLog(
            call_time=datetime.now(timezone.utc),
            endpoint=f"/jobs/{ADZUNA_COUNTRY}/search/{page}",
            page=page,
        )

        try:
            # Call API
            data = self.api.search_jobs(
                page=page, what=what, where=where, max_days_old=max_days_old
            )

            jobs = data.get("results", [])
            log_entry.jobs_fetched = len(jobs)
            log_entry.response_time_ms = data.get("_response_time_ms", 0)
            log_entry.status = "success"

            # Store jobs
            new_count = 0
            updated_count = 0

            for job_data in jobs:
                result = self._upsert_job(job_data)
                if result == "new":
                    new_count += 1
                elif result == "updated":
                    updated_count += 1

            self.db.commit()

            logger.debug(f"Page {page}: {len(jobs)} jobs, {new_count} new, {updated_count} updated")

            return {"jobs_count": len(jobs), "new": new_count, "updated": updated_count}

        except Exception as e:
            self.db.rollback()
            log_entry.status = "failed"
            log_entry.error_message = str(e)
            logger.error(f"Page {page} fetch failed: {e}")
            return None

        finally:
            self.db.add(log_entry)
            self.db.commit()

    def _upsert_job(self, job_data: dict) -> str:
        """
        Insert or update a job record

        Returns:
            "new" | "updated" | "unchanged"
        """
        adzuna_id = str(job_data.get("id", ""))

        if not adzuna_id:
            return "unchanged"

        existing = self.db.query(Job).filter(Job.adzuna_id == adzuna_id).first()

        # Parse creation date
        created_date = None
        if job_data.get("created"):
            try:
                created_date = datetime.fromisoformat(job_data["created"].replace("Z", "+00:00"))
            except ValueError:
                pass

        # Extract company name
        company_name = ""
        if job_data.get("company"):
            company_name = job_data["company"].get("display_name", "")

        # Extract location
        location = ""
        if job_data.get("location"):
            location = job_data["location"].get("display_name", "")

        # Extract category
        category = ""
        if job_data.get("category"):
            category = job_data["category"].get("label", "")

        if existing:
            # Update existing record
            existing.title = job_data.get("title", existing.title)
            existing.company_name = company_name or existing.company_name
            existing.category = category
            existing.location = location
            existing.salary_min = job_data.get("salary_min")
            existing.salary_max = job_data.get("salary_max")
            existing.description = job_data.get("description")
            existing.redirect_url = job_data.get("redirect_url")
            existing.created_date = created_date
            existing.is_active = True
            return "updated"
        else:
            # Create new record
            new_job = Job(
                adzuna_id=adzuna_id,
                title=job_data.get("title"),
                company_name=company_name,
                category=category,
                location=location,
                salary_min=job_data.get("salary_min"),
                salary_max=job_data.get("salary_max"),
                description=job_data.get("description"),
                redirect_url=job_data.get("redirect_url"),
                created_date=created_date,
            )
            self.db.add(new_job)
            return "new"

    def run_continuous(
        self, what: str = "", where: str = "", max_days_old: int = 7, interval_seconds: int = 1080
    ) -> None:
        """
        Run continuous fetch loop, fetching one page of the newest jobs at each interval.

        Designed for a monthly budget of 2400 requests:
        - 80 requests/day  (2400 / 30)
        - 1 request every 1080 seconds  (86400 / 80)

        Always fetches page 1 (most recently posted jobs sorted by date).
        Duplicate jobs are handled by upsert and will simply be updated.

        Args:
            what: Search keywords
            where: Location
            max_days_old: Only include jobs posted within the last N days
            interval_seconds: Seconds to wait between fetches (default 1080)
        """
        logger.info("=" * 60)
        logger.info("Starting continuous fetch loop")
        logger.info(f"Interval: {interval_seconds}s (~{interval_seconds / 60:.1f} min)")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)

        fetch_count = 0

        while True:
            # Check quota before fetching
            if not self.rate_limiter.wait_if_needed():
                logger.warning("Quota exhausted, stopping continuous fetch")
                break

            # Fetch page 1 (newest jobs)
            result = self._fetch_page(1, what, where, max_days_old)
            self.rate_limiter.record_call()
            fetch_count += 1

            if result:
                logger.info(
                    f"[Fetch #{fetch_count}] {result['jobs_count']} jobs, "
                    f"{result['new']} new, {result['updated']} updated"
                )
            else:
                logger.warning(f"[Fetch #{fetch_count}] Failed")

            # Show quota status every 10 fetches
            if fetch_count % 10 == 0:
                quota = self.rate_limiter.get_remaining_quota()
                logger.info(
                    f"Quota remaining: daily={quota['daily']['remaining']}, "
                    f"weekly={quota['weekly']['remaining']}, monthly={quota['monthly']['remaining']}"
                )

            next_fetch = datetime.now(timezone.utc).strftime("%H:%M:%S")
            logger.info(
                f"Next fetch in {interval_seconds}s (at ~{next_fetch} UTC + {interval_seconds}s)"
            )

            try:
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                logger.info(f"Stopped by user after {fetch_count} fetches")
                break

    def get_quota_status(self) -> dict:
        """Get current quota status"""
        return self.rate_limiter.get_remaining_quota()

    def get_jobs(self, category: str | None = None, limit: int = 50) -> list[Job]:
        """Get jobs from database"""
        query = self.db.query(Job).filter(Job.is_active.is_(True))

        if category:
            query = query.filter(Job.category.ilike(f"%{category}%"))

        return query.order_by(Job.created_date.desc()).limit(limit).all()


# ==================== Test Code ====================

if __name__ == "__main__":
    # Check configuration
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("Error: Please configure ADZUNA_APP_ID and ADZUNA_APP_KEY in .env")
        print("Get credentials at: https://developer.adzuna.com/")
        exit(1)

    service = JobFetchService()

    # Display current quota
    print("=== Current Quota Status ===")
    quota = service.get_quota_status()
    print(f"Daily quota: {quota['daily']['remaining']}/{quota['daily']['limit']}")
    print(f"Weekly quota: {quota['weekly']['remaining']}/{quota['weekly']['limit']}")
    print(f"Monthly quota: {quota['monthly']['remaining']}/{quota['monthly']['limit']}")

    # Start fetching
    print("\n=== Starting Batch Fetch ===")
    result = service.fetch_all_available(
        what="software developer",  # Modify search term as needed
        max_days_old=7,  # Jobs from the last 7 days
        max_pages=240,  # One day's allowance
    )

    print("\n=== Fetch Results ===")
    print(f"Status: {result['status']}")
    print(f"Pages fetched: {result['pages_fetched']}")
    print(f"Jobs fetched: {result['jobs_fetched']}")
    print(f"New: {result['jobs_new']}")
    print(f"Updated: {result['jobs_updated']}")

    # Display sample jobs
    print("\n=== Sample Jobs ===")
    jobs = service.get_jobs(limit=5)
    for job in jobs:
        salary = ""
        if job.salary_min and job.salary_max:
            salary = f"${job.salary_min:,.0f}-${job.salary_max:,.0f}"
        print(f"- {job.title} @ {job.company_name} | {salary}")
