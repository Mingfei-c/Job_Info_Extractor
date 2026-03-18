"""
FastAPI Application
Exposes resume analysis and job matching as a REST API
"""

import os
import tempfile

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from src.services.embedding_service import EmbeddingService
from src.services.gap_analyzer import GapAnalyzer
from src.services.job_fetch import JobFetchService
from src.services.resume_parser import ResumeParser

app = FastAPI(
    title="Job Info Extractor API",
    description="Resume analysis and job matching service",
    version="1.0.0",
)

# Lazy-initialized services (avoid loading at import time)
_embedding_service: EmbeddingService | None = None
_gap_analyzer: GapAnalyzer | None = None
_job_service: JobFetchService | None = None


def get_embedding_service() -> EmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service


def get_gap_analyzer() -> GapAnalyzer:
    global _gap_analyzer
    if _gap_analyzer is None:
        _gap_analyzer = GapAnalyzer()
    return _gap_analyzer


def get_job_service() -> JobFetchService:
    global _job_service
    if _job_service is None:
        _job_service = JobFetchService()
    return _job_service


# ==================== Endpoints ====================


@app.get("/health")
def health():
    """Service health check"""
    return {"status": "ok"}


@app.post("/analyze")
async def analyze_resume(file: UploadFile = File(...), top_k: int = 5):
    """
    Upload a resume (PDF or DOCX) and get:
    - Top matching jobs from the database
    - Gap analysis for the best match

    Args:
        file: Resume file (PDF or DOCX)
        top_k: Number of top matching jobs to return (default 5)
    """
    # Validate file type
    suffix = os.path.splitext(file.filename)[1].lower()
    if suffix not in (".pdf", ".docx"):
        raise HTTPException(status_code=400, detail="Only PDF and DOCX files are supported")

    # Save uploaded file to a temp path for the parser
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Parse resume
        parser = ResumeParser()
        resume = parser.parse(tmp_path)

        # Search matching jobs
        matches = get_embedding_service().search_similar_jobs(resume.raw_text, top_k=top_k)

        if not matches:
            return JSONResponse(
                content={"resume": _resume_summary(resume), "jobs": [], "gap_analysis": None}
            )

        # Gap analysis for the top match
        top_job = matches[0]
        gap = get_gap_analyzer().analyze(resume.raw_text, top_job)

        return {
            "resume": _resume_summary(resume),
            "jobs": [_job_summary(j) for j in matches],
            "gap_analysis": {
                "job_title": top_job.get("title"),
                "match_score": gap.match_score,
                "strengths": gap.strengths,
                "gaps": gap.gaps,
                "suggestions": gap.suggestions,
            },
        }

    finally:
        os.unlink(tmp_path)


@app.get("/jobs")
def get_jobs(limit: int = 20, category: str | None = None):
    """
    Get recent jobs from the database

    Args:
        limit: Maximum number of jobs to return (default 20)
        category: Filter by job category (optional)
    """
    jobs = get_job_service().get_jobs(category=category, limit=limit)
    return {"jobs": [_job_summary_from_model(j) for j in jobs], "count": len(jobs)}


# ==================== Helpers ====================


def _resume_summary(resume) -> dict:
    return {
        "file": os.path.basename(resume.file_path),
        "char_count": resume.char_count,
        "word_count": resume.word_count,
    }


def _job_summary(job: dict) -> dict:
    return {
        "id": job.get("id"),
        "title": job.get("title"),
        "company": job.get("company_name"),
        "location": job.get("location"),
        "similarity": round(job.get("similarity", 0), 4),
    }


def _job_summary_from_model(job) -> dict:
    return {
        "id": job.id,
        "title": job.title,
        "company": job.company_name,
        "location": job.location,
        "category": job.category,
        "salary_min": job.salary_min,
        "salary_max": job.salary_max,
    }
