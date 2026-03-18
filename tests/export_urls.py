"""
Export all redirect_urls from the adzuna_jobs table to a txt file
"""

import sys

sys.path.insert(0, ".")

from src.services.job_fetch import Job, SessionLocal


def export_redirect_urls(output_file: str = "redirect_urls.txt"):
    """
    Export redirect_url for all jobs in the database to a txt file
    One URL per line
    """
    db = SessionLocal()

    try:
        # Query all redirect_urls
        jobs = db.query(Job.redirect_url).filter(Job.redirect_url.isnot(None)).all()

        # Write to file
        with open(output_file, "w", encoding="utf-8") as f:
            for job in jobs:
                if job.redirect_url:
                    f.write(job.redirect_url + "\n")

        print(f"Exported {len(jobs)} URLs to {output_file}")

    finally:
        db.close()


if __name__ == "__main__":
    export_redirect_urls()
