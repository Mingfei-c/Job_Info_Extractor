"""
Chat Interface
Interactive chat interface for resume analysis
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv()

from src.agent.agent import create_resume_agent


def chat():
    """Start interactive chat"""
    print("=" * 60)
    print("Resume Analysis Assistant")
    print("=" * 60)
    print()
    print("I'm your career advisor assistant. I can help you:")
    print("  • Parse resumes (PDF/DOCX)")
    print("  • Search for matching jobs")
    print("  • Analyze gaps between resume and job requirements")
    print()
    print("Type 'quit' or 'exit' to exit")
    print("=" * 60)
    print()
    
    try:
        agent = create_resume_agent()
    except Exception as e:
        print(f"Initialization failed: {e}")
        return
    
    # Conversation history
    messages = []
    
    while True:
        try:
            # Get user input
            user_input = input("You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye! Good luck with your job search!")
                break
            
            # Add user message
            messages.append(("user", user_input))
            
            # Call Agent
            print("\nThinking...\n")
            
            result = agent.invoke({
                "messages": messages
            })
            
            # Get Agent's response
            agent_messages = result["messages"]
            
            # Find the last AI message
            for msg in reversed(agent_messages):
                if hasattr(msg, 'content') and msg.content:
                    if hasattr(msg, 'type') and msg.type == 'ai':
                        print(f"Assistant: {msg.content}")
                        messages.append(("assistant", msg.content))
                        break
            
            print()
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}\n")
            import traceback
            traceback.print_exc()


def run_single_query(query: str):
    """Execute a single query"""
    try:
        agent = create_resume_agent()
        
        result = agent.invoke({
            "messages": [("user", query)]
        })
        
        # Get the last AI message
        for msg in reversed(result["messages"]):
            if hasattr(msg, 'content') and msg.content:
                if hasattr(msg, 'type') and msg.type == 'ai':
                    print(msg.content)
                    return
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Resume Analysis Assistant")
    parser.add_argument("--query", "-q", type=str, help="Single query (non-interactive mode)")
    
    args = parser.parse_args()
    
    if args.query:
        run_single_query(args.query)
    else:
        chat()
