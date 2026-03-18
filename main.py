"""
Job Info Extractor - Main Entry Point

This is the main entry point for the application.
Run this file from the project root to ensure all imports work correctly.

Usage:
    python main.py chat          # Start interactive chat
    python main.py embed         # Run embedding service
    python main.py test          # Run tests
    python main.py serve         # Start FastAPI server
    python main.py fetch         # Continuous job fetch (every 1080s)
    python main.py fetch --once  # Fetch one page and exit (for cron)
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Job Info Extractor - Resume Analysis and Job Matching"
    )
    parser.add_argument(
        "command",
        choices=["chat", "embed", "test", "gap", "fetch", "serve"],
        help="Command to run: chat (interactive), embed (embedding service), test (run tests), gap (gap analyzer test), fetch (job fetch), serve (FastAPI server)",
    )
    parser.add_argument(
        "--query", "-q", type=str, help="Single query for chat (non-interactive mode)"
    )
    parser.add_argument(
        "--what", type=str, default="", help="Job search keywords (for fetch command)"
    )
    parser.add_argument(
        "--where", type=str, default="", help="Job search location (for fetch command)"
    )
    parser.add_argument(
        "--interval", type=int, default=1080, help="Seconds between fetches (default: 1080)"
    )
    parser.add_argument(
        "--once", action="store_true", help="Fetch one page and exit (for use with cron)"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host for serve command (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Port for serve command (default: 8000)"
    )

    args = parser.parse_args()

    if args.command == "chat":
        from src.agent.chat import chat, run_single_query

        if args.query:
            run_single_query(args.query)
        else:
            chat()

    elif args.command == "embed":
        from src.services.embedding_service import EmbeddingService

        print("=" * 60)
        print("Embedding Service")
        print("=" * 60)

        try:
            service = EmbeddingService()

            # Show statistics
            stats = service.get_stats()
            print("\nStatistics:")
            print(f"   Total jobs: {stats['total_jobs']}")
            print(f"   Embedded: {stats['embedded_jobs']}")
            print(f"   Pending: {stats['pending_jobs']}")

            # Run embedding
            print("\nStarting embedding...")
            result = service.embed_all_jobs(batch_size=50)
            print(f"   Processed: {result['processed']}")
            print(f"   Skipped: {result['skipped']}")
            print(f"   Failed: {result['failed']}")

        except Exception as e:
            print(f"Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    elif args.command == "gap":
        from src.services.gap_analyzer import GapAnalyzer

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
            """,
        }

        try:
            analyzer = GapAnalyzer()
            print("\nStarting analysis...\n")
            result = analyzer.analyze(test_resume, test_job)
            print(analyzer.format_report(result))
        except Exception as e:
            print(f"Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    elif args.command == "fetch":
        from src.services.job_fetch import JobFetchService

        try:
            service = JobFetchService()

            if args.once:
                # Fetch one page and exit — designed for cron
                print("Fetching one page...")
                result = service.fetch_all_available(what=args.what, where=args.where, max_pages=1)
                print(f"Done: {result['jobs_fetched']} jobs, {result['jobs_new']} new")
            else:
                # Continuous loop
                print("=" * 60)
                print("Continuous Job Fetch")
                print(
                    f"Interval: {args.interval}s | Keywords: '{args.what}' | Location: '{args.where}'"
                )
                print("=" * 60)
                service.run_continuous(
                    what=args.what,
                    where=args.where,
                    interval_seconds=args.interval,
                )

        except KeyboardInterrupt:
            print("\nStopped.")
        except Exception as e:
            print(f"Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    elif args.command == "serve":
        import uvicorn

        print("=" * 60)
        print(f"Starting API server at http://{args.host}:{args.port}")
        print(f"Docs: http://{args.host}:{args.port}/docs")
        print("=" * 60)

        uvicorn.run(
            "src.api.app:app",
            host=args.host,
            port=args.port,
            reload=False,
        )

    elif args.command == "test":
        print("=" * 60)
        print("Running Tests")
        print("=" * 60)

        # Run resume parser test
        print("\n--- Resume Parser Test ---")
        from tests.test_resume_parser import test_invalid_file, test_parse_docx

        result1 = test_parse_docx()
        result2 = test_invalid_file()

        print("\n--- Embedding Service Test ---")
        from tests.test_embedding_service import test_embedding_service

        result3 = test_embedding_service()

        print("\n" + "=" * 60)
        print("Test Results Summary")
        print("=" * 60)
        print(f"  DOCX parsing: {'Passed' if result1 else 'Failed'}")
        print(f"  Error handling: {'Passed' if result2 else 'Failed'}")
        print(f"  Embedding service: {'Passed' if result3 else 'Failed'}")

        if not all([result1, result2, result3]):
            sys.exit(1)


if __name__ == "__main__":
    main()
