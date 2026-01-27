"""
Chat Interface
Interactive chat interface for resume analysis
"""

from src.agent.agent import create_resume_agent


def chat():
    """Start interactive chat"""
    print("=" * 60)
    print("Resume Analysis Assistant")
    print("=" * 60)
    print()
    print("I can help you:")
    print("  - Parse your resume (PDF/Word format)")
    print("  - Search for jobs that match your skills")
    print("  - Analyze the gap between your resume and target jobs")
    print()
    print("Tips:")
    print(
        "  - You can upload your resume, such as: 'Please parse my resume resume/Mingfei Cao CV.docx'"
    )
    print("  - After parsing, you can ask me to search for matching jobs")
    print("  - Type 'quit' to exit")
    print("=" * 60)

    # Create Agent
    agent = create_resume_agent()

    # Conversation history
    messages = []

    while True:
        try:
            print()
            user_input = input("You: ").strip()

            if not user_input:
                continue

            if user_input.lower() in ["quit", "exit", "q"]:
                print("\nGoodbye! Good luck with your job search!")
                break

            # Add user message
            messages.append(("user", user_input))

            # Call Agent
            print("\nThinking...\n")

            result = agent.invoke({"messages": messages})

            # Get Agent's response
            agent_messages = result["messages"]

            # Find the last AI message
            for msg in reversed(agent_messages):
                if hasattr(msg, "content") and msg.content:
                    if hasattr(msg, "type") and msg.type == "ai":
                        print(f"Assistant: {msg.content}")
                        messages.append(("assistant", msg.content))
                        break

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}\n")
            import traceback

            traceback.print_exc()


def run_single_query(query: str):
    """Run a single query"""
    try:
        agent = create_resume_agent()

        result = agent.invoke({"messages": [("user", query)]})

        # Get the last AI message
        for msg in reversed(result["messages"]):
            if hasattr(msg, "content") and msg.content:
                if hasattr(msg, "type") and msg.type == "ai":
                    print(msg.content)
                    return

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
