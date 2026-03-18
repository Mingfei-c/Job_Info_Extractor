"""
Description Scraper Service
Access job redirect_url and extract full description to store in database
Includes rate limiting control
"""

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from src.services.job_fetch import Job

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ==================== Rate Limiting Configuration ====================


class ScrapeRateLimits:
    """Scraper rate limiting configuration (API returns 20 jobs per call, scraper runs 1 per page, so limits x20)"""

    # Sliding window limit
    WINDOW_SECONDS = 70  # 70-second window
    WINDOW_MAX_REQUESTS = 500  # Max 500 requests per window (25 x 20)

    # Natural time period limits (with buffer)
    DAILY_MAX = 4800  # Daily max (240 x 20)
    WEEKLY_MAX = 19200  # Weekly max (960 x 20)
    MONTHLY_MAX = 48000  # Monthly max (2400 x 20)


# ==================== Database Models ====================


class FullDescription(Base):
    """Full job description table"""

    __tablename__ = "full_descriptions"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, unique=True, index=True)  # References adzuna_jobs.id (maintained at application layer)
    full_description = Column(Text)
    source_url = Column(String(1000))
    scraped_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    html_length = Column(Integer)  # Raw HTML length
    status = Column(String(50))  # success, failed, redirected


class ScrapeLog(Base):
    """Scrape log table"""

    __tablename__ = "scrape_logs"

    id = Column(Integer, primary_key=True, index=True)
    scrape_time = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    job_id = Column(Integer)
    url = Column(String(1000))
    status = Column(String(50))  # success, failed
    response_time_ms = Column(Integer)
    error_message = Column(Text)


# Create tables
Base.metadata.create_all(bind=engine)


# ==================== Scrape Rate Limiter ====================


class ScrapeRateLimiter:
    """Scraper rate limiter (aligned with Adzuna API limits)"""

    def __init__(self, db_session):
        self.db = db_session
        self.recent_calls = deque()

    def _get_utc_now(self) -> datetime:
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

    def get_scrapes_in_period(self, start_time: datetime) -> int:
        """Get number of successful scrapes after a given timestamp"""
        count = (
            self.db.query(ScrapeLog)
            .filter(ScrapeLog.scrape_time >= start_time, ScrapeLog.status == "success")
            .count()
        )
        return count

    def get_remaining_quota(self) -> dict:
        """Get remaining quota"""
        now = self._get_utc_now()

        daily_used = self.get_scrapes_in_period(self._get_day_start(now))
        weekly_used = self.get_scrapes_in_period(self._get_week_start(now))
        monthly_used = self.get_scrapes_in_period(self._get_month_start(now))

        return {
            "daily": {
                "used": daily_used,
                "limit": ScrapeRateLimits.DAILY_MAX,
                "remaining": max(0, ScrapeRateLimits.DAILY_MAX - daily_used),
            },
            "weekly": {
                "used": weekly_used,
                "limit": ScrapeRateLimits.WEEKLY_MAX,
                "remaining": max(0, ScrapeRateLimits.WEEKLY_MAX - weekly_used),
            },
            "monthly": {
                "used": monthly_used,
                "limit": ScrapeRateLimits.MONTHLY_MAX,
                "remaining": max(0, ScrapeRateLimits.MONTHLY_MAX - monthly_used),
            },
        }

    def can_scrape(self) -> tuple[bool, str]:
        """Check whether a scrape request can be made"""
        now = time.time()

        # Remove expired sliding window records
        window_start = now - ScrapeRateLimits.WINDOW_SECONDS
        while self.recent_calls and self.recent_calls[0] < window_start:
            self.recent_calls.popleft()

        # Check sliding window
        if len(self.recent_calls) >= ScrapeRateLimits.WINDOW_MAX_REQUESTS:
            wait_time = self.recent_calls[0] + ScrapeRateLimits.WINDOW_SECONDS - now
            return False, f"Sliding window limit: wait {wait_time:.1f} seconds"

        # Check daily/weekly/monthly limits
        quota = self.get_remaining_quota()

        if quota["daily"]["remaining"] <= 0:
            return False, "Daily limit reached (4800 requests)"

        if quota["weekly"]["remaining"] <= 0:
            return False, "Weekly limit reached (19200 requests)"

        if quota["monthly"]["remaining"] <= 0:
            return False, "Monthly limit reached (48000 requests)"

        return True, "OK"

    def record_request(self):
        """Record one request in the sliding window"""
        self.recent_calls.append(time.time())

    def wait_if_needed(self) -> bool:
        """Wait if necessary, returns whether scraping can continue"""
        while True:
            can_request, reason = self.can_scrape()

            if can_request:
                return True

            # Stop if daily/weekly/monthly limit reached
            if "Daily limit" in reason or "Weekly limit" in reason or "Monthly limit" in reason:
                logger.warning(f"Stopping scrape: {reason}")
                return False

            # Wait for sliding window limit
            logger.debug(f"Waiting: {reason}")
            time.sleep(5)


# ==================== Description Extractor ====================


class DescriptionExtractor:
    """Extracts job descriptions from web pages"""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    # EEO truncation keywords
    CUTOFF_KEYWORDS = [
        "eeo statement",
        "equal employment opportunity",
        "equal opportunity employer",
        "we are an equal opportunity",
        "e-verify",
        "eeoc",
        "affirmative action",
        "discrimination policy",
    ]

    @classmethod
    def extract(cls, url: str) -> dict:
        """
        Extract job description from a URL

        Returns:
            {"description": str, "status": str, "html_length": int, "error": str}
        """
        try:
            start_time = time.time()
            response = requests.get(url, headers=cls.HEADERS, timeout=15, allow_redirects=True)
            response_time = int((time.time() - start_time) * 1000)

            if response.status_code != 200:
                return {
                    "description": "",
                    "status": "failed",
                    "html_length": 0,
                    "response_time_ms": response_time,
                    "error": f"HTTP {response.status_code}",
                }

            html_length = len(response.text)
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract description
            description = cls._extract_description(soup)

            # Remove EEO content
            description = cls._clean_description(description)

            return {
                "description": description,
                "status": "success" if description else "no_content",
                "html_length": html_length,
                "response_time_ms": response_time,
                "error": "",
            }

        except requests.Timeout:
            return {
                "description": "",
                "status": "timeout",
                "html_length": 0,
                "error": "Request timeout",
            }
        except requests.RequestException as e:
            return {"description": "", "status": "failed", "html_length": 0, "error": str(e)}

    @classmethod
    def _extract_description(cls, soup: BeautifulSoup) -> str:
        """Extract description from HTML"""
        description = ""

        # Method 1: find divs with "description" in class name
        for div in soup.find_all("div", class_=True):
            classes = " ".join(div.get("class", []))
            if "description" in classes.lower() or "job-content" in classes.lower():
                text = div.get_text(separator="\n", strip=True)
                if len(text) > len(description):
                    description = text

        # Method 2: find article or main elements
        if not description:
            for tag in ["article", "main", "section"]:
                element = soup.find(tag)
                if element:
                    text = element.get_text(separator="\n", strip=True)
                    if len(text) > 200:
                        description = text
                        break

        return description

    @classmethod
    def _clean_description(cls, text: str) -> str:
        """Clean description by removing EEO content"""
        if not text:
            return text

        text_lower = text.lower()
        earliest_idx = len(text)

        for keyword in cls.CUTOFF_KEYWORDS:
            idx = text_lower.find(keyword)
            if idx != -1 and idx < earliest_idx:
                earliest_idx = idx

        if earliest_idx < len(text):
            return text[:earliest_idx].strip()

        return text


# ==================== Description Scrape Service ====================


class DescriptionScrapeService:
    """Description scraping service"""

    def __init__(self):
        self.db = SessionLocal()
        self.rate_limiter = ScrapeRateLimiter(self.db)

    def __del__(self):
        self.db.close()

    def scrape_pending_jobs(self, max_jobs: int = 100) -> dict:
        """
        Scrape full descriptions for all unscraped jobs

        Args:
            max_jobs: Maximum number of jobs to scrape in this run

        Returns:
            Scrape result statistics
        """
        logger.info("=" * 60)
        logger.info("Starting full description scrape")

        # Query unscraped jobs
        pending_jobs = (
            self.db.query(Job)
            .filter(Job.is_scraped.is_(False), Job.redirect_url.isnot(None))
            .limit(max_jobs)
            .all()
        )

        logger.info(f"Found {len(pending_jobs)} pending jobs")

        if not pending_jobs:
            return {"status": "no_pending", "scraped": 0, "success": 0, "failed": 0}

        # Statistics
        scraped = 0
        success = 0
        failed = 0

        for job in pending_jobs:
            # Check rate limit
            if not self.rate_limiter.wait_if_needed():
                logger.warning("Daily limit reached, stopping scrape")
                break

            # Scrape
            result = self._scrape_job(job)
            scraped += 1

            if result["status"] == "success":
                success += 1
            else:
                failed += 1

            # Record request
            self.rate_limiter.record_request()

            # Progress log
            if scraped % 10 == 0:
                logger.info(f"Progress: {scraped}/{len(pending_jobs)}, success: {success}, failed: {failed}")

        result = {"status": "completed", "scraped": scraped, "success": success, "failed": failed}

        logger.info("=" * 60)
        logger.info(f"Scrape complete: {scraped} total, {success} success, {failed} failed")

        return result

    def _scrape_job(self, job: Job) -> dict:
        """Scrape a single job"""
        log_entry = ScrapeLog(
            scrape_time=datetime.now(timezone.utc), job_id=job.id, url=job.redirect_url
        )

        try:
            # Extract description
            result = DescriptionExtractor.extract(job.redirect_url)

            log_entry.status = result["status"]
            log_entry.response_time_ms = result.get("response_time_ms", 0)
            log_entry.error_message = result.get("error", "")

            # Store full description
            if result["description"]:
                full_desc = FullDescription(
                    job_id=job.id,
                    full_description=result["description"],
                    source_url=job.redirect_url,
                    html_length=result["html_length"],
                    status=result["status"],
                )
                self.db.add(full_desc)

            # Mark as scraped
            job.is_scraped = True

            self.db.commit()

            logger.debug(
                f"Scraped job_id={job.id}: {result['status']}, {len(result['description'])} chars"
            )

            return result

        except Exception as e:
            self.db.rollback()
            log_entry.status = "error"
            log_entry.error_message = str(e)
            logger.error(f"Failed to scrape job_id={job.id}: {e}")
            return {"status": "error", "error": str(e)}

        finally:
            self.db.add(log_entry)
            self.db.commit()

    def get_scrape_stats(self) -> dict:
        """Get scrape statistics"""
        total_jobs = self.db.query(Job).count()
        scraped_jobs = self.db.query(Job).filter(Job.is_scraped.is_(True)).count()
        pending_jobs = self.db.query(Job).filter(Job.is_scraped.is_(False)).count()
        descriptions = self.db.query(FullDescription).count()
        today_scrapes = self.rate_limiter.get_remaining_quota()["daily"]["used"]

        return {
            "total_jobs": total_jobs,
            "scraped_jobs": scraped_jobs,
            "pending_jobs": pending_jobs,
            "full_descriptions": descriptions,
            "today_scrapes": today_scrapes,
            "daily_limit": ScrapeRateLimits.DAILY_MAX,
        }


# ==================== Test Code ====================

if __name__ == "__main__":
    service = DescriptionScrapeService()

    # Display statistics
    print("=== Scrape Statistics ===")
    stats = service.get_scrape_stats()
    print(f"Total jobs: {stats['total_jobs']}")
    print(f"Scraped: {stats['scraped_jobs']}")
    print(f"Pending: {stats['pending_jobs']}")
    print(f"Today's scrapes: {stats['today_scrapes']}/{stats['daily_limit']}")

    # Start scraping
    print("\n=== Starting Scrape ===")
    result = service.scrape_pending_jobs(max_jobs=10000)  # Scrape all pending jobs

    print("\n=== Results ===")
    print(f"Status: {result['status']}")
    print(f"Scraped: {result['scraped']}")
    print(f"Success: {result['success']}")
    print(f"Failed: {result['failed']}")
