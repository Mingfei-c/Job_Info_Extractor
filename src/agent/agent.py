"""
ReAct Agent
Resume analysis agent built with LangGraph
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv()

from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI

from src.agent.tools import ALL_TOOLS

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
    
    # Create Gemini model
    model = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=GEMINI_API_KEY,
        temperature=0.3,
    )
    
    # System prompt
    system_prompt = """You are a professional career advisor AI assistant, helping users analyze resumes and match suitable job opportunities.

You can use the following tools:

1. **parse_resume**: Parse user's resume file (PDF or DOCX format)
2. **search_jobs**: Search for jobs matching the resume
3. **analyze_gap**: Analyze the gap between resume and a specific job
4. **get_job_details**: Get detailed job description

## Workflow

When a user requests resume analysis, follow these steps:

1. First use `parse_resume` to parse the user's resume
2. Then use `search_jobs` to search for matching jobs
3. If the user wants to know details about a job, use `get_job_details`
4. If the user wants to know the gap with a job, use `analyze_gap`

## Notes

- Respond in English
- Provide clear, professional advice
- If user doesn't specify number of jobs, default to searching 10
- When analyzing gaps, give specific actionable improvement suggestions
"""
    
    # Create ReAct Agent
    agent = create_react_agent(
        model=model,
        tools=ALL_TOOLS,
        prompt=system_prompt,
    )
    
    return agent


# ==================== Test ====================

if __name__ == "__main__":
    print("=" * 60)
    print("ReAct Agent Test")
    print("=" * 60)
    
    try:
        agent = create_resume_agent()
        print("Agent created successfully!")
        
        # Test conversation
        test_message = "Hello, what can you do?"
        print(f"\nUser: {test_message}")
        
        result = agent.invoke({
            "messages": [("user", test_message)]
        })
        
        # Get last message
        last_message = result["messages"][-1]
        print(f"\nAgent: {last_message.content}")
        
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("\nPlease install required dependencies:")
        print("  pip install langgraph langchain-google-genai")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
