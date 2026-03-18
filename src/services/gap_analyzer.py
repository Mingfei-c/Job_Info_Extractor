"""
Gap Analyzer Service
Analyze gaps between resume and job requirements using Gemini 2.5 Flash
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Gemini API Key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


@dataclass
class GapAnalysis:
    """Gap analysis result"""

    job_id: int
    job_title: str
    company_name: str
    match_score: int  # 0-100 match score
    strengths: list[str]  # Resume strengths
    gaps: list[str]  # Gaps/weaknesses
    suggestions: list[str]  # Improvement suggestions
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

1. **Match Score** (0-100): Overall match score, considering skills, experience, and education
2. **Strengths**: List 3-5 resume strengths relevant to this job
3. **Gaps**: List 3-5 gaps or weaknesses that need improvement
4. **Suggestions**: Give 3-5 specific actionable improvement suggestions

Output in JSON format:
```json
{
    "match_score": 75,
    "strengths": [
        "Proficient in Python programming",
        "Experience with data analysis"
    ],
    "gaps": [
        "Lacking cloud platform experience",
        "No team management experience"
    ],
    "suggestions": [
        "Recommend getting AWS certification",
        "Highlight relevant projects in resume"
    ]
}
```

Notes:
- Be objective and accurate, don't overpraise or underestimate
- Suggestions should be specific and actionable
- Consider both hard skills and soft skills
- Language of response should match the resume language
"""

    def __init__(self):
        """Initialize service"""
        if not GEMINI_API_KEY:
            raise ValueError("Please configure GEMINI_API_KEY in .env")

        try:
            from google import genai

            self.client = genai.Client(api_key=GEMINI_API_KEY)
        except ImportError as err:
            raise ImportError("google-genai required: pip install google-genai") from err

    def analyze(self, resume_text: str, job: dict) -> GapAnalysis:
        """
        Analyze gaps between resume and job

        Args:
            resume_text: Resume text content
            job: Job info dict, should include id, title, company_name, description

        Returns:
            GapAnalysis: Gap analysis result
        """
        logger.info(
            f"Analyzing job: {job.get('title', 'Unknown')} @ {job.get('company_name', 'Unknown')}"
        )

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
                },
            )

            raw_response = response.text
            logger.debug(f"LLM response: {(raw_response or '')[:200]}...")

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
                raw_response=raw_response,
            )

        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise

    def _build_prompt(self, resume_text: str, job: dict) -> str:
        """Build analysis prompt"""
        # Build job info
        job_info = f"""
## Job Information

- **Job Title**: {job.get("title", "N/A")}
- **Company Name**: {job.get("company_name", "N/A")}
- **Location**: {job.get("location", "N/A")}
- **Category**: {job.get("category", "N/A")}
"""

        if job.get("salary_min") and job.get("salary_max"):
            job_info += (
                f"- **Salary Range**: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}\n"
            )

        # Prefer full_description, otherwise use description
        description = job.get("full_description") or job.get("description", "No description")

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

        # Remove possible markdown code block markers
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]

        if response.endswith("```"):
            response = response[:-3]

        response = response.strip()

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            logger.debug(f"Raw response: {response}")
            # Return default values
            return {
                "match_score": 0,
                "strengths": ["Parse failed"],
                "gaps": ["Parse failed"],
                "suggestions": ["Please retry"],
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
                results.append(self.analyze(resume_text, job))
            except Exception as e:
                logger.error(f"Failed to analyze job {job.get('id')}: {e}")
                continue

        # Sort by match score
        results.sort(key=lambda x: x.match_score, reverse=True)

        return results

    def format_report(self, analysis: GapAnalysis) -> str:
        """Format analysis report"""
        report = f"""
============================================================
{analysis.job_title} @ {analysis.company_name}
============================================================

Match Score: {analysis.match_score}%

Strengths:
"""
        for s in analysis.strengths:
            report += f"   - {s}\n"

        report += "\nGaps:\n"
        for g in analysis.gaps:
            report += f"   - {g}\n"

        report += "\nSuggestions:\n"
        for s in analysis.suggestions:
            report += f"   - {s}\n"

        return report
