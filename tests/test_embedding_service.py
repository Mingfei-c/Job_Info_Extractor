"""
Test Embedding Service
"""

import importlib.util


def test_embedding_service():
    """Test Embedding service"""

    print("=" * 60)
    print("Test Embedding Service")
    print("=" * 60)

    # Check dependencies using importlib.util.find_spec
    print("\nChecking dependencies...")

    if importlib.util.find_spec("langchain_google_genai"):
        print("  langchain-google-genai installed")
    else:
        print("  Required: pip install langchain-google-genai")
        return False

    if importlib.util.find_spec("pgvector"):
        print("  pgvector installed")
    else:
        print("  Required: pip install pgvector")
        return False

    # Test initialization
    print("\nTesting initialization...")

    try:
        from src.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        print("  EmbeddingService initialized successfully")
    except ValueError as e:
        print(f"  Config error: {e}")
        return False
    except Exception as e:
        print(f"  Initialization failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Test embedding generation
    print("\nTesting embedding generation...")

    try:
        test_text = "Python developer with 5 years experience in machine learning and data science"
        vector = service.generate_embedding(test_text)

        print(f"  Input text: '{test_text[:50]}...'")
        print(f"  Vector dimension: {len(vector)}")
        print(f"  Sample values: {vector[:5]}")

        if len(vector) != 768:
            print(f"  Warning: expected 768 dimensions, got {len(vector)}")

        print("  Embedding generation successful")

    except Exception as e:
        print(f"  Embedding generation failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Test batch embedding (small batch)
    print("\nTesting batch embedding...")

    try:
        result = service.embed_all_jobs(batch_size=5)
        print(f"  Processed: {result['processed']}")
        print(f"  Skipped: {result['skipped']}")
        print(f"  Failed: {result['failed']}")

        if result["processed"] > 0 or result["skipped"] > 0:
            print("  Batch embedding successful")
        else:
            print("  No jobs to process (database may be empty)")

    except Exception as e:
        print(f"  Batch processing failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Test vector search
    print("\nTesting vector search...")

    # Update statistics
    stats = service.get_stats()
    if stats["embedded_jobs"] == 0:
        print("  No embedded jobs, skipping search test")
    else:
        try:
            query = "Software engineer with Python and AWS experience"
            jobs = service.search_similar_jobs(query, top_k=3)

            print(f"  Query: '{query}'")
            print(f"  Found: {len(jobs)} jobs")

            for i, job in enumerate(jobs, 1):
                print(f"  {i}. {job['title']} @ {job['company_name']}")
                print(f"     Similarity: {job['similarity']:.2%}")

            print("  Vector search successful")

        except Exception as e:
            print(f"  Search failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    print("\n" + "=" * 60)
    print("All Embedding Service tests passed!")
    print("=" * 60)

    return True


def test_resume_matching():
    """Test matching with actual resume"""
    from pathlib import Path

    project_root = Path(__file__).parent.parent
    resume_path = project_root / "resume" / "Mingfei Cao CV.docx"

    if not resume_path.exists():
        print("Resume file not found, skipping matching test")
        return False

    print("=" * 60)
    print("Resume Matching Test")
    print("=" * 60)

    try:
        from src.services.embedding_service import EmbeddingService
        from src.services.resume_parser import ResumeParser

        # Parse resume
        print("\nParsing resume...")
        resume = ResumeParser.parse(str(resume_path))
        print(f"  Character count: {resume.char_count}")

        # Initialize embedding service
        service = EmbeddingService()

        # Check if there are embedded jobs
        stats = service.get_stats()
        if stats["embedded_jobs"] == 0:
            print("  No embedded jobs, please run embed_all_jobs() first")
            return False

        # Search for matching jobs
        print("\nSearching for matching jobs...")
        jobs = service.search_similar_jobs(resume.raw_text, top_k=5)

        print("\nTop 5 matching jobs:")
        for i, job in enumerate(jobs, 1):
            print(f"  {i}. {job['title']}")
            print(f"     Company: {job['company_name']}")
            print(f"     Location: {job['location']}")
            print(f"     Similarity: {job['similarity']:.2%}")
            if job["salary_min"] and job["salary_max"]:
                print(f"     Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}")

        print("\nResume matching test successful!")
        return True

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False
