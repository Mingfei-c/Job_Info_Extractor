"""
Agent Tools
Define tools available to the ReAct Agent
"""

from typing import Optional

from langchain_core.tools import tool

from src.services.embedding_service import EmbeddingService
from src.services.gap_analyzer import GapAnalyzer
from src.services.resume_parser import ResumeData, ResumeParser

# ==================== Global State ====================
# Store parsed resume and matched jobs

_current_resume: ResumeData | None = None
_matched_jobs: list[dict] = []


# ==================== Initialization ====================
# Lazy initialization to avoid errors when not needed

_embedding_service: EmbeddingService | None = None
_gap_analyzer: GapAnalyzer | None = None


def _get_embedding_service() -> EmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service


def _get_gap_analyzer() -> GapAnalyzer:
    global _gap_analyzer
    if _gap_analyzer is None:
        _gap_analyzer = GapAnalyzer()
    return _gap_analyzer


# ==================== Tool Definitions ====================


@tool
def parse_resume(file_path: str) -> str:
    """
    Parse a resume file (PDF or DOCX format) and extract text content.

    Supported formats: PDF (.pdf), Word (.docx)

    Args:
        file_path: Path to the resume file

    Returns:
        Summary of parsing result including character and word count
    """
    global _current_resume

    try:
        resume = ResumeParser.parse(file_path)
        _current_resume = resume

        summary = f"""Resume parsed successfully!

File: {resume.file_path}
Format: {resume.file_type.upper()}
Character count: {resume.char_count:,}
Word count: {resume.word_count:,}

Content preview (first 500 characters):
---
{resume.raw_text[:500]}...
---

The resume content has been saved and is ready for job matching."""

        return summary

    except FileNotFoundError:
        return f"Error: File not found: {file_path}\nPlease check if the file path is correct."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        return f"Error parsing resume: {e}"


@tool
def search_jobs(query: Optional[str] = None, top_k: int = 10) -> str:
    """
    Search for jobs matching the resume.

    Uses vector similarity to search for the most matching jobs in the database.
    If a resume has been parsed, it will automatically use the resume content as the search query.

    Args:
        query: Search query text (optional, defaults to using the parsed resume)
        top_k: Number of jobs to return, default 10

    Returns:
        List of matching jobs
    """
    global _matched_jobs

    # Determine search query
    if query:
        search_text = query
    elif _current_resume:
        search_text = _current_resume.raw_text
    else:
        return "Error: No resume or search query provided. Please parse a resume first or provide a search query."

    try:
        service = _get_embedding_service()
        jobs = service.search_similar_jobs(search_text, top_k=top_k)

        if not jobs:
            return "No matching jobs found. Try different search terms."

        _matched_jobs = jobs

        result = f"Found {len(jobs)} matching jobs:\n\n"

        for i, job in enumerate(jobs, 1):
            result += f"""**{i}. {job["title"]}**
   Company: {job["company_name"]}
   Location: {job["location"]}
   Similarity: {job["similarity"]:.1%}
"""
            if job.get("salary_min") and job.get("salary_max"):
                result += f"   Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"
            result += "\n"

        result += "\nTip: Use the `analyze_gap` tool (with job number) to view gap analysis for a specific job."

        return result

    except Exception as e:
        return f"Search error: {e}"


@tool
def analyze_gap(job_index: int) -> str:
    """
    Analyze the gap between resume and a specific job.

    Performs detailed match analysis on a searched job, identifying strengths, gaps, and improvement suggestions.

    Args:
        job_index: Job number (starting from 1, corresponding to the search_jobs result list)

    Returns:
        Detailed gap analysis report
    """
    if not _current_resume:
        return "Error: Please parse a resume first."

    if not _matched_jobs:
        return "Error: Please use search_jobs to search for jobs first."

    if job_index < 1 or job_index > len(_matched_jobs):
        return f"Error: Invalid job number. Please enter 1-{len(_matched_jobs)}."

    job = _matched_jobs[job_index - 1]

    try:
        analyzer = _get_gap_analyzer()
        analysis = analyzer.analyze(_current_resume.raw_text, job)

        return analyzer.format_report(analysis)

    except Exception as e:
        return f"Analysis error: {e}"


@tool
def get_job_details(job_index: int) -> str:
    """
    Get detailed job description.

    Args:
        job_index: Job number (starting from 1)

    Returns:
        Full job description
    """
    if not _matched_jobs:
        return "Error: Please use search_jobs to search for jobs first."

    if job_index < 1 or job_index > len(_matched_jobs):
        return f"Error: Invalid job number. Please enter 1-{len(_matched_jobs)}."

    job = _matched_jobs[job_index - 1]

    description = job.get("full_description") or job.get("description", "No description")

    result = f"""
============================================================
{job["title"]}
============================================================

Company: {job["company_name"]}
Location: {job["location"]}
Category: {job.get("category", "N/A")}
"""

    if job.get("salary_min") and job.get("salary_max"):
        result += f"Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"

    if job.get("redirect_url"):
        result += f"Link: {job['redirect_url']}\n"

    result += f"""
------------------------------------------------------------

{description}
"""

    return result


# Export all tools
ALL_TOOLS = [
    parse_resume,
    search_jobs,
    analyze_gap,
    get_job_details,
]
