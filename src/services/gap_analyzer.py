"""
Gap Analyzer Service
Analyze gaps between resume and job requirements using Gemini 2.5 Flash
"""

import os
import sys
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Gemini API Key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


@dataclass
class GapAnalysis:
    """Gap analysis result"""
    job_id: int
    job_title: str
    company_name: str
    match_score: int              # 0-100 match score
    strengths: list[str]          # Resume strengths
    gaps: list[str]               # Gaps/weaknesses
    suggestions: list[str]        # Improvement suggestions
    raw_response: Optional[str] = None  # Raw LLM response


class GapAnalyzer:
    """
    Resume-Job Gap Analysis Service
    
    Uses Gemini 2.5 Flash to analyze the match and gaps between resume and job requirements
    
    Usage:
        analyzer = GapAnalyzer()
        result = analyzer.analyze(resume_text, job_dict)
        print(f"Match score: {result.match_score}%")
        print(f"Gaps: {result.gaps}")
    """
    
    MODEL = "gemini-2.5-flash"
    
    SYSTEM_PROMPT = """You are a professional recruitment consultant and career planner. Your task is to analyze the match between a candidate's resume and the target job.

Based on the provided resume and job description, perform the following analysis:

1. **Match Score** (0-100)
   - 90-100: Excellent match, meets all core requirements
   - 70-89: Good match, meets most requirements
   - 50-69: Moderate match, clear gaps but addressable
   - 30-49: Weak match, needs significant improvement
   - 0-29: Not a match, suggest looking for other positions

2. **Resume Strengths** (matching points with job requirements)
   - List skills, experience, education, etc. from the resume that match job requirements

3. **Gap Analysis** (job requirements missing or insufficient in resume)
   - Clearly identify missing skills, insufficient experience years, education requirements, etc.

4. **Improvement Suggestions** (how to bridge gaps)
   - Provide specific actionable suggestions, such as learning paths, project ideas, certifications, etc.

Return results in JSON format as follows:
```json
{
    "match_score": 75,
    "strengths": ["...", "..."],
    "gaps": ["...", "..."],
    "suggestions": ["...", "..."]
}
```

Notes:
- Only return JSON, no other content
- Be specific in analysis, avoid generalities
- Suggestions should be actionable, not abstract"""

    def __init__(self):
        """Initialize service"""
        if not GEMINI_API_KEY:
            raise ValueError("Please configure GEMINI_API_KEY in .env")
        
        try:
            from google import genai
            self.client = genai.Client(api_key=GEMINI_API_KEY)
        except ImportError:
            raise ImportError("google-genai required: pip install google-genai")
    
    def analyze(self, resume_text: str, job: dict) -> GapAnalysis:
        """
        Analyze gaps between resume and job
        
        Args:
            resume_text: Resume text content
            job: Job info dict, should include id, title, company_name, description
            
        Returns:
            GapAnalysis: Gap analysis result
        """
        logger.info(f"Analyzing job: {job.get('title', 'Unknown')} @ {job.get('company_name', 'Unknown')}")
        
        # Build prompt
        prompt = self._build_prompt(resume_text, job)
        
        try:
            # Call Gemini API
            response = self.client.models.generate_content(
                model=self.MODEL,
                contents=prompt,
                config={
                    "system_instruction": self.SYSTEM_PROMPT,
                    "temperature": 0,  # Lower temperature for more stable output
                }
            )
            
            raw_response = response.text
            logger.debug(f"LLM response: {raw_response[:200]}...")
            
            # Parse JSON response
            result = self._parse_response(raw_response)
            
            return GapAnalysis(
                job_id=job.get("id", 0),
                job_title=job.get("title", "Unknown"),
                company_name=job.get("company_name", "Unknown"),
                match_score=result.get("match_score", 0),
                strengths=result.get("strengths", []),
                gaps=result.get("gaps", []),
                suggestions=result.get("suggestions", []),
                raw_response=raw_response
            )
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise
    
    def _build_prompt(self, resume_text: str, job: dict) -> str:
        """Build analysis prompt"""
        # Build job info
        job_info = f"""
## Job Information

- **Job Title**: {job.get('title', 'N/A')}
- **Company Name**: {job.get('company_name', 'N/A')}
- **Location**: {job.get('location', 'N/A')}
- **Category**: {job.get('category', 'N/A')}
"""
        
        if job.get('salary_min') and job.get('salary_max'):
            job_info += f"- **Salary Range**: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"
        
        # Prefer full_description, otherwise use description
        description = job.get('full_description') or job.get('description', 'No description')
        
        job_info += f"""
### Job Description

{description}
"""
        
        prompt = f"""
{job_info}

---

## Candidate Resume

{resume_text}

---

Please analyze the match between this resume and the above job, and provide detailed gap analysis.
"""
        return prompt
    
    def _parse_response(self, response: str) -> dict:
        """Parse LLM JSON response"""
        # Try to extract JSON
        response = response.strip()
        
        # Remove possible markdown code block markers
        if response.startswith("```json"):
            response = response[7:]
        elif response.startswith("```"):
            response = response[3:]
        
        if response.endswith("```"):
            response = response[:-3]
        
        response = response.strip()
        
        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e}")
            logger.error(f"Raw response: {response}")
            # Return default values
            return {
                "match_score": 0,
                "strengths": ["Parse failed"],
                "gaps": ["Parse failed"],
                "suggestions": ["Please retry"]
            }
    
    def analyze_multiple(self, resume_text: str, jobs: list[dict]) -> list[GapAnalysis]:
        """
        Analyze multiple jobs
        
        Args:
            resume_text: Resume text
            jobs: Job list
            
        Returns:
            List of gap analysis results, sorted by match score descending
        """
        results = []
        
        for i, job in enumerate(jobs, 1):
            logger.info(f"Analysis progress: {i}/{len(jobs)}")
            try:
                result = self.analyze(resume_text, job)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to analyze job {job.get('id')}: {e}")
                continue
        
        # Sort by match score
        results.sort(key=lambda x: x.match_score, reverse=True)
        
        return results
    
    def format_report(self, analysis: GapAnalysis) -> str:
        """Format analysis report"""
        report = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{analysis.job_title} @ {analysis.company_name}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Match Score: {analysis.match_score}%

Strengths:
"""
        for s in analysis.strengths:
            report += f"   • {s}\n"
        
        report += "\nGaps:\n"
        for g in analysis.gaps:
            report += f"   • {g}\n"
        
        report += "\nSuggestions:\n"
        for s in analysis.suggestions:
            report += f"   • {s}\n"
        
        return report


# ==================== Test Code ====================

if __name__ == "__main__":
    print("=" * 60)
    print("Gap Analyzer Test")
    print("=" * 60)
    
    # Test resume text
    test_resume = """
    John Doe
    Software Developer
    
    Skills: Python, JavaScript, React, SQL, Git
    Experience: 2 years at TechCorp as Junior Developer
    Education: BS in Computer Science, State University, 2022
    
    Projects:
    - Built a web application using React and Node.js
    - Developed REST APIs with Python Flask
    """
    
    # Test job
    test_job = {
        "id": 1,
        "title": "Senior Python Developer",
        "company_name": "BigTech Inc",
        "location": "San Francisco, CA",
        "category": "IT Jobs",
        "salary_min": 120000,
        "salary_max": 180000,
        "description": """
        We are looking for a Senior Python Developer with:
        - 5+ years of Python experience
        - Experience with Django or FastAPI
        - Knowledge of AWS or GCP cloud services
        - Experience with Docker and Kubernetes
        - Strong SQL and database design skills
        - Experience leading a team is a plus
        
        Responsibilities:
        - Design and develop scalable backend systems
        - Mentor junior developers
        - Code review and architecture decisions
        """
    }
    
    try:
        analyzer = GapAnalyzer()
        
        print("\nStarting analysis...\n")
        result = analyzer.analyze(test_resume, test_job)
        
        print(analyzer.format_report(result))
        
    except ValueError as e:
        print(f"Configuration error: {e}")
    except ImportError as e:
        print(f"Missing dependency: {e}")
    except Exception as e:
        print(f"Analysis failed: {e}")
        import traceback
        traceback.print_exc()
