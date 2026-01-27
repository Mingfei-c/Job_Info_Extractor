"""
导出 adzuna_jobs 表中所有 redirect_url 到 txt 文件
"""

import sys

sys.path.insert(0, ".")

from src.services.job_fetch import Job, SessionLocal


def export_redirect_urls(output_file: str = "redirect_urls.txt"):
    """
    将数据库中所有职位的 redirect_url 导出到 txt 文件
    每行一个 URL
    """
    db = SessionLocal()

    try:
        # 查询所有 redirect_url
        jobs = db.query(Job.redirect_url).filter(Job.redirect_url.isnot(None)).all()

        # 写入文件
        with open(output_file, "w", encoding="utf-8") as f:
            for job in jobs:
                if job.redirect_url:
                    f.write(job.redirect_url + "\n")

        print(f"已导出 {len(jobs)} 个 URL 到 {output_file}")

    finally:
        db.close()


if __name__ == "__main__":
    export_redirect_urls()
