"""
Job Fetch Service - Adzuna API
从 Adzuna API 获取职位并存储到 PostgreSQL 数据库
包含完整的速率限制控制
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from collections import deque

import requests
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float, func
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库配置
DATABASE_URL = os.getenv("DATABASE_URL")  # 必须在 .env 中配置
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Adzuna API 配置
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
ADZUNA_COUNTRY = os.getenv("ADZUNA_COUNTRY", "us")  # 默认美国


# ==================== 速率限制配置 ====================

class RateLimits:
    """速率限制配置"""
    # 滑动窗口限制
    WINDOW_SECONDS = 70          # 70秒窗口
    WINDOW_MAX_REQUESTS = 25     # 每窗口最多25次
    
    # 自然时间段限制（留余量）
    DAILY_MAX = 240              # 每日最多（250的余量）
    WEEKLY_MAX = 960             # 每周最多（1000的余量）
    MONTHLY_MAX = 2400           # 每月最多（2500的余量）
    
    # 每次请求返回的职位数
    JOBS_PER_REQUEST = 20


# ==================== 数据库模型 ====================

class Job(Base):
    """职位表"""
    __tablename__ = "adzuna_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    adzuna_id = Column(String(100), unique=True, index=True)  # Adzuna 职位 ID
    title = Column(String(500), nullable=False)
    company_name = Column(String(255))
    category = Column(String(100))
    location = Column(String(255))
    salary_min = Column(Float)
    salary_max = Column(Float)
    description = Column(Text)
    redirect_url = Column(String(1000))
    created_date = Column(DateTime)  # Adzuna 上的发布时间
    fetched_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=True)
    is_scraped = Column(Boolean, default=False, index=True)  # 是否已爬取完整描述


class ApiCallLog(Base):
    """API 调用日志表 - 记录每次调用的时间"""
    __tablename__ = "api_call_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    call_time = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    endpoint = Column(String(255))
    page = Column(Integer)
    jobs_fetched = Column(Integer, default=0)
    status = Column(String(50))  # success, failed
    error_message = Column(Text)
    response_time_ms = Column(Integer)


# 创建表
Base.metadata.create_all(bind=engine)


# ==================== 速率限制器 ====================

class RateLimiter:
    """
    速率限制器
    - 滑动窗口：70秒内最多25次
    - 自然日/周/月限制
    """
    
    def __init__(self, db_session):
        self.db = db_session
        self.recent_calls = deque()  # 滑动窗口记录
    
    def _get_utc_now(self) -> datetime:
        """获取当前 UTC 时间"""
        return datetime.now(timezone.utc)
    
    def _get_day_start(self, dt: datetime) -> datetime:
        """获取自然日开始时间（UTC）"""
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def _get_week_start(self, dt: datetime) -> datetime:
        """获取自然周开始时间（UTC，周一为起点）"""
        day_start = self._get_day_start(dt)
        days_since_monday = dt.weekday()
        return day_start.replace(day=dt.day - days_since_monday)
    
    def _get_month_start(self, dt: datetime) -> datetime:
        """获取自然月开始时间（UTC）"""
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    def get_calls_in_period(self, start_time: datetime) -> int:
        """获取某时间点之后的调用次数"""
        count = self.db.query(func.count(ApiCallLog.id)).filter(
            ApiCallLog.call_time >= start_time,
            ApiCallLog.status == "success"
        ).scalar()
        return count or 0
    
    def get_remaining_quota(self) -> dict:
        """获取剩余配额"""
        now = self._get_utc_now()
        
        # 计算各时间段的使用量
        daily_used = self.get_calls_in_period(self._get_day_start(now))
        weekly_used = self.get_calls_in_period(self._get_week_start(now))
        monthly_used = self.get_calls_in_period(self._get_month_start(now))
        
        return {
            "daily": {
                "used": daily_used,
                "limit": RateLimits.DAILY_MAX,
                "remaining": max(0, RateLimits.DAILY_MAX - daily_used)
            },
            "weekly": {
                "used": weekly_used,
                "limit": RateLimits.WEEKLY_MAX,
                "remaining": max(0, RateLimits.WEEKLY_MAX - weekly_used)
            },
            "monthly": {
                "used": monthly_used,
                "limit": RateLimits.MONTHLY_MAX,
                "remaining": max(0, RateLimits.MONTHLY_MAX - monthly_used)
            }
        }
    
    def can_make_request(self) -> tuple[bool, str]:
        """
        检查是否可以发起请求
        Returns: (可以请求, 原因)
        """
        now = self._get_utc_now()
        
        # 清理过期的滑动窗口记录
        window_start = now.timestamp() - RateLimits.WINDOW_SECONDS
        while self.recent_calls and self.recent_calls[0] < window_start:
            self.recent_calls.popleft()
        
        # 检查滑动窗口
        if len(self.recent_calls) >= RateLimits.WINDOW_MAX_REQUESTS:
            wait_time = self.recent_calls[0] + RateLimits.WINDOW_SECONDS - now.timestamp()
            return False, f"滑动窗口限制：需等待 {wait_time:.1f} 秒"
        
        # 检查日/周/月限制
        quota = self.get_remaining_quota()
        
        if quota["daily"]["remaining"] <= 0:
            return False, "已达到每日限制 (240次)"
        
        if quota["weekly"]["remaining"] <= 0:
            return False, "已达到每周限制 (960次)"
        
        if quota["monthly"]["remaining"] <= 0:
            return False, "已达到每月限制 (2400次)"
        
        return True, "OK"
    
    def record_call(self):
        """记录一次调用（滑动窗口）"""
        self.recent_calls.append(self._get_utc_now().timestamp())
    
    def wait_if_needed(self) -> bool:
        """
        如果需要等待，则等待
        Returns: True 如果可以继续，False 如果达到日/周/月限制
        """
        while True:
            can_request, reason = self.can_make_request()
            
            if can_request:
                return True
            
            # 如果是日/周/月限制，不等待
            if "每日" in reason or "每周" in reason or "每月" in reason:
                logger.warning(f"停止获取：{reason}")
                return False
            
            # 滑动窗口限制，等待
            logger.info(f"速率限制：{reason}")
            time.sleep(5)  # 每5秒检查一次


# ==================== Adzuna API 客户端 ====================

class AdzunaAPI:
    """Adzuna API 客户端"""
    
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    
    def __init__(self, app_id: str, app_key: str, country: str = "us"):
        self.app_id = app_id
        self.app_key = app_key
        self.country = country
    
    def search_jobs(self, page: int = 1, what: str = "", where: str = "", 
                    max_days_old: int = None) -> dict:
        """
        搜索职位
        
        Args:
            page: 页码（从1开始）
            what: 搜索关键词
            where: 地点
            max_days_old: 只获取最近N天的职位
        
        Returns:
            API 响应数据
        """
        url = f"{self.BASE_URL}/{self.country}/search/{page}"
        
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": RateLimits.JOBS_PER_REQUEST,
            "sort_by": "date"
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


# ==================== 职位获取服务 ====================

class JobFetchService:
    """职位获取和存储服务"""
    
    def __init__(self):
        self.db = SessionLocal()
        self.rate_limiter = RateLimiter(self.db)
        self.api = AdzunaAPI(ADZUNA_APP_ID, ADZUNA_APP_KEY, ADZUNA_COUNTRY)
    
    def __del__(self):
        self.db.close()
    
    def fetch_all_available(self, what: str = "", where: str = "", 
                            max_days_old: int = 7, max_pages: int = 240) -> dict:
        """
        一次性获取尽可能多的职位（在不触及限制的情况下）
        
        Args:
            what: 搜索关键词
            where: 地点
            max_days_old: 只获取最近N天的职位
            max_pages: 最大获取页数（默认240，一天的量）
        
        Returns:
            获取结果统计
        """
        logger.info("=" * 60)
        logger.info("开始批量获取职位")
        logger.info(f"搜索条件: what='{what}', where='{where}', max_days_old={max_days_old}")
        
        # 检查配额
        quota = self.rate_limiter.get_remaining_quota()
        logger.info(f"当前配额: 日={quota['daily']['remaining']}, "
                   f"周={quota['weekly']['remaining']}, 月={quota['monthly']['remaining']}")
        
        # 计算实际可获取的页数
        available_requests = min(
            quota["daily"]["remaining"],
            quota["weekly"]["remaining"],
            quota["monthly"]["remaining"],
            max_pages
        )
        
        if available_requests <= 0:
            logger.warning("没有可用配额，停止获取")
            return {
                "status": "no_quota",
                "pages_fetched": 0,
                "jobs_fetched": 0,
                "jobs_new": 0,
                "jobs_updated": 0
            }
        
        logger.info(f"计划获取 {available_requests} 页")
        
        # 开始获取
        total_jobs_fetched = 0
        jobs_new = 0
        jobs_updated = 0
        pages_fetched = 0
        
        for page in range(1, available_requests + 1):
            # 检查并等待速率限制
            if not self.rate_limiter.wait_if_needed():
                break
            
            # 发起请求
            result = self._fetch_page(page, what, where, max_days_old)
            
            if result is None:
                logger.error(f"第 {page} 页获取失败，停止")
                break
            
            pages_fetched += 1
            total_jobs_fetched += result["jobs_count"]
            jobs_new += result["new"]
            jobs_updated += result["updated"]
            
            # 记录调用
            self.rate_limiter.record_call()
            
            # 如果没有更多职位，停止
            if result["jobs_count"] < RateLimits.JOBS_PER_REQUEST:
                logger.info("没有更多职位，停止获取")
                break
            
            # 进度日志
            if page % 10 == 0:
                logger.info(f"进度: {page}/{available_requests} 页, "
                           f"已获取 {total_jobs_fetched} 个职位")
        
        # 最终统计
        result = {
            "status": "success",
            "pages_fetched": pages_fetched,
            "jobs_fetched": total_jobs_fetched,
            "jobs_new": jobs_new,
            "jobs_updated": jobs_updated,
            "quota_remaining": self.rate_limiter.get_remaining_quota()
        }
        
        logger.info("=" * 60)
        logger.info(f"获取完成: {pages_fetched} 页, {total_jobs_fetched} 个职位, "
                   f"{jobs_new} 新增, {jobs_updated} 更新")
        
        return result
    
    def _fetch_page(self, page: int, what: str, where: str, 
                    max_days_old: int) -> Optional[dict]:
        """
        获取单页职位并存储
        
        Returns:
            {"jobs_count": N, "new": N, "updated": N} 或 None（失败）
        """
        log_entry = ApiCallLog(
            call_time=datetime.now(timezone.utc),
            endpoint=f"/jobs/{ADZUNA_COUNTRY}/search/{page}",
            page=page
        )
        
        try:
            # 调用 API
            data = self.api.search_jobs(
                page=page,
                what=what,
                where=where,
                max_days_old=max_days_old
            )
            
            jobs = data.get("results", [])
            log_entry.jobs_fetched = len(jobs)
            log_entry.response_time_ms = data.get("_response_time_ms", 0)
            log_entry.status = "success"
            
            # 存储职位
            new_count = 0
            updated_count = 0
            
            for job_data in jobs:
                result = self._upsert_job(job_data)
                if result == "new":
                    new_count += 1
                elif result == "updated":
                    updated_count += 1
            
            self.db.commit()
            
            logger.debug(f"第 {page} 页: {len(jobs)} 职位, {new_count} 新增, {updated_count} 更新")
            
            return {
                "jobs_count": len(jobs),
                "new": new_count,
                "updated": updated_count
            }
            
        except Exception as e:
            self.db.rollback()
            log_entry.status = "failed"
            log_entry.error_message = str(e)
            logger.error(f"第 {page} 页获取失败: {e}")
            return None
            
        finally:
            self.db.add(log_entry)
            self.db.commit()
    
    def _upsert_job(self, job_data: dict) -> str:
        """
        插入或更新职位
        
        Returns:
            "new" | "updated" | "unchanged"
        """
        adzuna_id = str(job_data.get("id", ""))
        
        if not adzuna_id:
            return "unchanged"
        
        existing = self.db.query(Job).filter(Job.adzuna_id == adzuna_id).first()
        
        # 解析创建日期
        created_date = None
        if job_data.get("created"):
            try:
                created_date = datetime.fromisoformat(
                    job_data["created"].replace("Z", "+00:00")
                )
            except ValueError:
                pass
        
        # 提取公司名
        company_name = ""
        if job_data.get("company"):
            company_name = job_data["company"].get("display_name", "")
        
        # 提取地点
        location = ""
        if job_data.get("location"):
            location = job_data["location"].get("display_name", "")
        
        # 提取类别
        category = ""
        if job_data.get("category"):
            category = job_data["category"].get("label", "")
        
        if existing:
            # 更新现有记录
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
            # 创建新记录
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
                created_date=created_date
            )
            self.db.add(new_job)
            return "new"
    
    def get_quota_status(self) -> dict:
        """获取当前配额状态"""
        return self.rate_limiter.get_remaining_quota()
    
    def get_jobs(self, category: str = None, limit: int = 50) -> list[Job]:
        """获取数据库中的职位"""
        query = self.db.query(Job).filter(Job.is_active == True)
        
        if category:
            query = query.filter(Job.category.ilike(f"%{category}%"))
        
        return query.order_by(Job.created_date.desc()).limit(limit).all()


# ==================== 测试代码 ====================

if __name__ == "__main__":
    # 检查配置
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("错误: 请在 .env 中配置 ADZUNA_APP_ID 和 ADZUNA_APP_KEY")
        print("获取地址: https://developer.adzuna.com/")
        exit(1)
    
    service = JobFetchService()
    
    # 显示当前配额
    print("=== 当前配额状态 ===")
    quota = service.get_quota_status()
    print(f"日配额: {quota['daily']['remaining']}/{quota['daily']['limit']}")
    print(f"周配额: {quota['weekly']['remaining']}/{quota['weekly']['limit']}")
    print(f"月配额: {quota['monthly']['remaining']}/{quota['monthly']['limit']}")
    
    # 开始获取
    print("\n=== 开始批量获取 ===")
    result = service.fetch_all_available(
        what="software developer",  # 可以修改搜索词
        max_days_old=7,             # 最近7天的职位
        max_pages=240               # 一天的量
    )
    
    print(f"\n=== 获取结果 ===")
    print(f"状态: {result['status']}")
    print(f"获取页数: {result['pages_fetched']}")
    print(f"获取职位数: {result['jobs_fetched']}")
    print(f"新增: {result['jobs_new']}")
    print(f"更新: {result['jobs_updated']}")
    
    # 显示部分职位
    print("\n=== 部分职位 ===")
    jobs = service.get_jobs(limit=5)
    for job in jobs:
        salary = ""
        if job.salary_min and job.salary_max:
            salary = f"${job.salary_min:,.0f}-${job.salary_max:,.0f}"
        print(f"- {job.title} @ {job.company_name} | {salary}")
