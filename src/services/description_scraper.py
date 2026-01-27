"""
Description Scraper Service
访问职位的 redirect_url 并提取完整描述存储到数据库
包含速率限制控制
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from collections import deque
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests
from bs4 import BeautifulSoup
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, create_engine, func
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

# 导入 Job 模型
from src.services.job_fetch import Job, Base as JobBase


# ==================== 速率限制配置 ====================

class ScrapeRateLimits:
    """爬虫速率限制配置（API单次返回20个职位，爬虫单次1页，所以限制×20）"""
    # 滑动窗口限制
    WINDOW_SECONDS = 70          # 70秒窗口
    WINDOW_MAX_REQUESTS = 500    # 每窗口最多500次（25×20）
    
    # 自然时间段限制（留余量）
    DAILY_MAX = 4800             # 每日最多（240×20）
    WEEKLY_MAX = 19200           # 每周最多（960×20）
    MONTHLY_MAX = 48000          # 每月最多（2400×20）


# ==================== 数据库模型 ====================

class FullDescription(Base):
    """完整职位描述表"""
    __tablename__ = "full_descriptions"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, unique=True, index=True)  # 关联 adzuna_jobs.id（应用层维护）
    full_description = Column(Text)
    source_url = Column(String(1000))
    scraped_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    html_length = Column(Integer)  # 原始 HTML 长度
    status = Column(String(50))    # success, failed, redirected


class ScrapeLog(Base):
    """爬虫日志表"""
    __tablename__ = "scrape_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    scrape_time = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    job_id = Column(Integer)
    url = Column(String(1000))
    status = Column(String(50))    # success, failed
    response_time_ms = Column(Integer)
    error_message = Column(Text)


# 创建表
Base.metadata.create_all(bind=engine)


# ==================== 爬虫速率限制器 ====================

class ScrapeRateLimiter:
    """爬虫速率限制器（与 Adzuna API 限制一致）"""
    
    def __init__(self, db_session):
        self.db = db_session
        self.recent_calls = deque()
    
    def _get_utc_now(self) -> datetime:
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
    
    def get_scrapes_in_period(self, start_time: datetime) -> int:
        """获取某时间点之后的爬取次数"""
        count = self.db.query(ScrapeLog).filter(
            ScrapeLog.scrape_time >= start_time,
            ScrapeLog.status == "success"
        ).count()
        return count
    
    def get_remaining_quota(self) -> dict:
        """获取剩余配额"""
        now = self._get_utc_now()
        
        daily_used = self.get_scrapes_in_period(self._get_day_start(now))
        weekly_used = self.get_scrapes_in_period(self._get_week_start(now))
        monthly_used = self.get_scrapes_in_period(self._get_month_start(now))
        
        return {
            "daily": {"used": daily_used, "limit": ScrapeRateLimits.DAILY_MAX, 
                     "remaining": max(0, ScrapeRateLimits.DAILY_MAX - daily_used)},
            "weekly": {"used": weekly_used, "limit": ScrapeRateLimits.WEEKLY_MAX,
                      "remaining": max(0, ScrapeRateLimits.WEEKLY_MAX - weekly_used)},
            "monthly": {"used": monthly_used, "limit": ScrapeRateLimits.MONTHLY_MAX,
                       "remaining": max(0, ScrapeRateLimits.MONTHLY_MAX - monthly_used)}
        }
    
    def can_scrape(self) -> tuple[bool, str]:
        """检查是否可以发起请求"""
        now = time.time()
        
        # 清理过期的滑动窗口记录
        window_start = now - ScrapeRateLimits.WINDOW_SECONDS
        while self.recent_calls and self.recent_calls[0] < window_start:
            self.recent_calls.popleft()
        
        # 检查滑动窗口
        if len(self.recent_calls) >= ScrapeRateLimits.WINDOW_MAX_REQUESTS:
            wait_time = self.recent_calls[0] + ScrapeRateLimits.WINDOW_SECONDS - now
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
    
    def record_request(self):
        """记录一次请求（滑动窗口）"""
        self.recent_calls.append(time.time())
    
    def wait_if_needed(self) -> bool:
        """如果需要等待则等待，返回是否可以继续"""
        while True:
            can_request, reason = self.can_scrape()
            
            if can_request:
                return True
            
            # 日/周/月限制则停止
            if "每日" in reason or "每周" in reason or "每月" in reason:
                logger.warning(f"停止爬取：{reason}")
                return False
            
            # 滑动窗口限制则等待
            logger.debug(f"等待：{reason}")
            time.sleep(5)


# ==================== 描述提取器 ====================

class DescriptionExtractor:
    """从网页提取职位描述"""
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    # EEO 截断关键词
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
        从 URL 提取职位描述
        
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
                    "error": f"HTTP {response.status_code}"
                }
            
            html_length = len(response.text)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取描述
            description = cls._extract_description(soup)
            
            # 清理 EEO 内容
            description = cls._clean_description(description)
            
            return {
                "description": description,
                "status": "success" if description else "no_content",
                "html_length": html_length,
                "response_time_ms": response_time,
                "error": ""
            }
            
        except requests.Timeout:
            return {"description": "", "status": "timeout", "html_length": 0, "error": "Request timeout"}
        except requests.RequestException as e:
            return {"description": "", "status": "failed", "html_length": 0, "error": str(e)}
    
    @classmethod
    def _extract_description(cls, soup: BeautifulSoup) -> str:
        """从 HTML 提取描述"""
        description = ""
        
        # 方法1: 查找包含 "description" 的 class
        for div in soup.find_all('div', class_=True):
            classes = ' '.join(div.get('class', []))
            if 'description' in classes.lower() or 'job-content' in classes.lower():
                text = div.get_text(separator='\n', strip=True)
                if len(text) > len(description):
                    description = text
        
        # 方法2: 查找 article 或 main
        if not description:
            for tag in ['article', 'main', 'section']:
                element = soup.find(tag)
                if element:
                    text = element.get_text(separator='\n', strip=True)
                    if len(text) > 200:
                        description = text
                        break
        
        return description
    
    @classmethod
    def _clean_description(cls, text: str) -> str:
        """清理描述，删除 EEO 内容"""
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


# ==================== 描述爬取服务 ====================

class DescriptionScrapeService:
    """描述爬取服务"""
    
    def __init__(self):
        self.db = SessionLocal()
        self.rate_limiter = ScrapeRateLimiter(self.db)
    
    def __del__(self):
        self.db.close()
    
    def scrape_pending_jobs(self, max_jobs: int = 100) -> dict:
        """
        爬取所有未爬取的职位描述
        
        Args:
            max_jobs: 本次最多爬取数量
        
        Returns:
            爬取结果统计
        """
        logger.info("=" * 60)
        logger.info("开始爬取职位完整描述")
        
        # 查询未爬取的职位
        pending_jobs = self.db.query(Job).filter(
            Job.is_scraped == False,
            Job.redirect_url.isnot(None)
        ).limit(max_jobs).all()
        
        logger.info(f"找到 {len(pending_jobs)} 个待爬取职位")
        
        if not pending_jobs:
            return {"status": "no_pending", "scraped": 0, "success": 0, "failed": 0}
        
        # 统计
        scraped = 0
        success = 0
        failed = 0
        
        for job in pending_jobs:
            # 检查速率限制
            if not self.rate_limiter.wait_if_needed():
                logger.warning("达到每日限制，停止爬取")
                break
            
            # 爬取
            result = self._scrape_job(job)
            scraped += 1
            
            if result["status"] == "success":
                success += 1
            else:
                failed += 1
            
            # 记录请求
            self.rate_limiter.record_request()
            
            # 进度日志
            if scraped % 10 == 0:
                logger.info(f"进度: {scraped}/{len(pending_jobs)}, 成功: {success}, 失败: {failed}")
        
        result = {
            "status": "completed",
            "scraped": scraped,
            "success": success,
            "failed": failed
        }
        
        logger.info("=" * 60)
        logger.info(f"爬取完成: {scraped} 个, 成功 {success}, 失败 {failed}")
        
        return result
    
    def _scrape_job(self, job: Job) -> dict:
        """爬取单个职位"""
        log_entry = ScrapeLog(
            scrape_time=datetime.now(timezone.utc),
            job_id=job.id,
            url=job.redirect_url
        )
        
        try:
            # 提取描述
            result = DescriptionExtractor.extract(job.redirect_url)
            
            log_entry.status = result["status"]
            log_entry.response_time_ms = result.get("response_time_ms", 0)
            log_entry.error_message = result.get("error", "")
            
            # 存储完整描述
            if result["description"]:
                full_desc = FullDescription(
                    job_id=job.id,
                    full_description=result["description"],
                    source_url=job.redirect_url,
                    html_length=result["html_length"],
                    status=result["status"]
                )
                self.db.add(full_desc)
            
            # 标记已爬取
            job.is_scraped = True
            
            self.db.commit()
            
            logger.debug(f"已爬取 job_id={job.id}: {result['status']}, {len(result['description'])} 字符")
            
            return result
            
        except Exception as e:
            self.db.rollback()
            log_entry.status = "error"
            log_entry.error_message = str(e)
            logger.error(f"爬取 job_id={job.id} 失败: {e}")
            return {"status": "error", "error": str(e)}
            
        finally:
            self.db.add(log_entry)
            self.db.commit()
    
    def get_scrape_stats(self) -> dict:
        """获取爬取统计"""
        total_jobs = self.db.query(Job).count()
        scraped_jobs = self.db.query(Job).filter(Job.is_scraped == True).count()
        pending_jobs = self.db.query(Job).filter(Job.is_scraped == False).count()
        descriptions = self.db.query(FullDescription).count()
        today_scrapes = self.rate_limiter.get_remaining_quota()["daily"]["used"]
        
        return {
            "total_jobs": total_jobs,
            "scraped_jobs": scraped_jobs,
            "pending_jobs": pending_jobs,
            "full_descriptions": descriptions,
            "today_scrapes": today_scrapes,
            "daily_limit": ScrapeRateLimits.DAILY_MAX
        }


# ==================== 测试代码 ====================

if __name__ == "__main__":
    service = DescriptionScrapeService()
    
    # 显示统计
    print("=== 爬取统计 ===")
    stats = service.get_scrape_stats()
    print(f"总职位数: {stats['total_jobs']}")
    print(f"已爬取: {stats['scraped_jobs']}")
    print(f"待爬取: {stats['pending_jobs']}")
    print(f"今日已爬取: {stats['today_scrapes']}/{stats['daily_limit']}")
    
    # 开始爬取
    print("\n=== 开始爬取 ===")
    result = service.scrape_pending_jobs(max_jobs=10000)  # 爬取所有待爬取的职位
    
    print(f"\n=== 结果 ===")
    print(f"状态: {result['status']}")
    print(f"爬取数: {result['scraped']}")
    print(f"成功: {result['success']}")
    print(f"失败: {result['failed']}")
