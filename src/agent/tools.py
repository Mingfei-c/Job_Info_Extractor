"""
Agent Tools
Define tools available to the ReAct Agent
"""

import os
import sys
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from langchain_core.tools import tool

from src.services.resume_parser import ResumeParser, ResumeData
from src.services.embedding_service import EmbeddingService
from src.services.gap_analyzer import GapAnalyzer


# ==================== Global Service Instances ====================

_embedding_service: Optional[EmbeddingService] = None
_gap_analyzer: Optional[GapAnalyzer] = None
_current_resume: Optional[ResumeData] = None
_matched_jobs: Optional[list[dict]] = None


def get_embedding_service() -> EmbeddingService:
    """Get EmbeddingService singleton"""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service


def get_gap_analyzer() -> GapAnalyzer:
    """Get GapAnalyzer singleton"""
    global _gap_analyzer
    if _gap_analyzer is None:
        _gap_analyzer = GapAnalyzer()
    return _gap_analyzer


# ==================== Tool Definitions ====================

@tool
def parse_resume(file_path: str) -> str:
    """
    Parse resume file and extract text content.
    
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
        
        # Return summary
        preview = resume.raw_text[:500] + "..." if len(resume.raw_text) > 500 else resume.raw_text
        
        return f"""Resume parsed successfully!

File: {Path(file_path).name}
Characters: {resume.char_count}
Words: {resume.word_count}

Content preview:
{preview}
"""
    except FileNotFoundError:
        return f"Error: File not found: {file_path}"
    except ValueError as e:
        return f"Error: Format error: {e}"
    except Exception as e:
        return f"Error: Parse failed: {e}"


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
    global _current_resume, _matched_jobs
    
    # Determine search text
    if query:
        search_text = query
    elif _current_resume:
        search_text = _current_resume.raw_text
    else:
        return "Error: Please first use parse_resume to parse a resume, or provide a search query"
    
    try:
        service = get_embedding_service()
        jobs = service.search_similar_jobs(search_text, top_k=top_k)
        _matched_jobs = jobs
        
        if not jobs:
            return "No matching jobs found. Please ensure there is job data in the database and it has been vectorized."
        
        # Format results
        result = f"Found {len(jobs)} matching jobs:\n\n"
        
        for i, job in enumerate(jobs, 1):
            result += f"""**{i}. {job['title']}**
   Company: {job['company_name']}
   Location: {job['location']}
   Similarity: {job['similarity']:.1%}
"""
            if job.get('salary_min') and job.get('salary_max'):
                result += f"   Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"
            result += "\n"
        
        return result
        
    except Exception as e:
        return f"Error: Search failed: {e}"


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
    global _current_resume, _matched_jobs
    
    if not _current_resume:
        return "Error: Please first use parse_resume to parse a resume"
    
    if not _matched_jobs:
        return "Error: Please first use search_jobs to search for jobs"
    
    if job_index < 1 or job_index > len(_matched_jobs):
        return f"Error: Invalid job number. Valid range: 1-{len(_matched_jobs)}"
    
    job = _matched_jobs[job_index - 1]
    
    try:
        analyzer = get_gap_analyzer()
        analysis = analyzer.analyze(_current_resume.raw_text, job)
        
        return analyzer.format_report(analysis)
        
    except Exception as e:
        return f"Error: Analysis failed: {e}"


@tool
def get_job_details(job_index: int) -> str:
    """
    Get detailed job description.
    
    Args:
        job_index: Job number (starting from 1)
        
    Returns:
        Full job description
    """
    global _matched_jobs
    
    if not _matched_jobs:
        return "Error: Please first use search_jobs to search for jobs"
    
    if job_index < 1 or job_index > len(_matched_jobs):
        return f"Error: Invalid job number. Valid range: 1-{len(_matched_jobs)}"
    
    job = _matched_jobs[job_index - 1]
    
    description = job.get('full_description') or job.get('description', 'No description')
    
    result = f"""
============================================================
{job['title']}
============================================================

Company: {job['company_name']}
Location: {job['location']}
Category: {job.get('category', 'N/A')}
"""
    
    if job.get('salary_min') and job.get('salary_max'):
        result += f"Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"
    
    if job.get('redirect_url'):
        result += f"Link: {job['redirect_url']}\n"
    
    result += f"""
============================================================

{description}
"""
    return result


# ==================== Tool List ====================

ALL_TOOLS = [
    parse_resume,
    search_jobs,
    analyze_gap,
    get_job_details,
]


# ==================== Test ====================

if __name__ == "__main__":
    print("Available tools:")
    for t in ALL_TOOLS:
        print(f"  - {t.name}: {t.description[:50]}...")
