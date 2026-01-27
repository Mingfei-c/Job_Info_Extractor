"""
ReAct Agent
Resume analysis agent built with LangGraph
"""

import os

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent

from src.agent.tools import ALL_TOOLS

load_dotenv()

# Gemini API Key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


def create_resume_agent():
    """
    Create resume analysis agent

    Returns:
        Compiled LangGraph Agent
    """
    if not GEMINI_API_KEY:
        raise ValueError("Please configure GEMINI_API_KEY in .env")

    # Create Gemini LLM
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=GEMINI_API_KEY,
        temperature=0,
    )

    # System prompt
    system_prompt = """You are a professional career consultant AI assistant, helping users analyze resumes and find matching jobs.

You have the following tools:
1. parse_resume - Parse resume files (PDF/DOCX format)
2. search_jobs - Search for jobs matching the resume
3. analyze_gap - Analyze the gap between resume and a specific job
4. get_job_details - Get detailed job description

Recommended workflow:
1. When the user provides a resume, first use parse_resume to parse
2. When the user wants to find matching jobs, use search_jobs to search
3. If the user is interested in a specific job, use analyze_gap for gap analysis
4. You can also use get_job_details to view detailed job requirements

Notes:
- Always communicate in the same language as the user
- Stay friendly and professional
- Provide practical advice"""

    # Create ReAct Agent
    agent = create_react_agent(
        model=llm,
        tools=ALL_TOOLS,
        prompt=system_prompt,
    )

    return agent
