"""
Test Embedding Service
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_embedding_service():
    """Test Embedding service"""
    
    print("=" * 60)
    print("Test Embedding Service")
    print("=" * 60)
    
    # Check dependencies
    print("\nChecking dependencies...")
    
    try:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        print("  langchain-google-genai installed")
    except ImportError:
        print("  Required: pip install langchain-google-genai")
        return False
    
    try:
        from pgvector.sqlalchemy import Vector
        print("  pgvector installed")
    except ImportError:
        print("  Required: pip install pgvector")
        return False
    
    # Initialize service
    print("\nInitializing service...")
    
    try:
        from src.services.embedding_service import EmbeddingService
        service = EmbeddingService()
        print("  EmbeddingService initialized successfully")
    except ValueError as e:
        print(f"  Configuration error: {e}")
        return False
    except Exception as e:
        print(f"  Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Get statistics
    print("\nDatabase statistics...")
    stats = service.get_stats()
    print(f"  Total jobs: {stats['total_jobs']}")
    print(f"  Embedded: {stats['embedded_jobs']}")
    print(f"  Pending: {stats['pending_jobs']}")
    
    # Test generating single embedding
    print("\nTesting Embedding generation...")
    try:
        test_text = "Python developer with 3 years of experience in machine learning"
        vector = service.generate_embedding(test_text)
        print(f"  Input text: '{test_text}'")
        print(f"  Vector dimension: {len(vector)}")
        print(f"  First 5 dimensions: {vector[:5]}")
        print("  Embedding generation successful")
    except Exception as e:
        print(f"  Generation failed: {e}")
        return False
    
    # Test batch embedding (only process a few)
    print("\nTesting batch embedding (max 5)...")
    try:
        result = service.embed_all_jobs(batch_size=5)
        print(f"  Processed: {result['processed']}")
        print(f"  Skipped: {result['skipped']}")
        print(f"  Failed: {result['failed']}")
        
        if result['processed'] > 0 or result['skipped'] > 0:
            print("  Batch embedding successful")
        else:
            print("  No jobs to process (database may be empty)")
    except Exception as e:
        print(f"  Batch processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test similarity search
    print("\nTesting similarity search...")
    
    # Update statistics
    stats = service.get_stats()
    if stats['embedded_jobs'] == 0:
        print("  No embedded jobs, skipping search test")
    else:
        try:
            query = "Software engineer with Python and AWS experience"
            jobs = service.search_similar_jobs(query, top_k=3)
            
            print(f"  Query: '{query}'")
            print(f"  Found {len(jobs)} matches:")
            
            for i, job in enumerate(jobs, 1):
                print(f"\n  {i}. {job['title']}")
                print(f"     Company: {job['company_name']}")
                print(f"     Similarity: {job['similarity']:.2%}")
            
            print("\n  Similarity search successful")
        except Exception as e:
            print(f"  Search failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)
    return True


def test_with_resume():
    """Test job search using resume"""
    
    print("\n" + "=" * 60)
    print("Test: Job Search Using Resume")
    print("=" * 60)
    
    resume_path = project_root / "resume" / "Mingfei Cao CV.docx"
    
    if not resume_path.exists():
        print(f"Resume file not found: {resume_path}")
        return False
    
    try:
        # Parse resume
        from src.services.resume_parser import ResumeParser
        from src.services.embedding_service import EmbeddingService
        
        print(f"\nParsing resume: {resume_path.name}")
        resume = ResumeParser.parse(str(resume_path))
        print(f"  Characters: {resume.char_count}")
        
        # Search matching jobs
        print("\nSearching matching jobs...")
        service = EmbeddingService()
        
        # Check if there are embedded jobs
        stats = service.get_stats()
        if stats['embedded_jobs'] == 0:
            print("  No embedded jobs, please run embed_all_jobs() first")
            return False
        
        jobs = service.search_similar_jobs(resume.raw_text, top_k=5)
        
        print(f"\nTop 5 Matching Jobs:")
        for i, job in enumerate(jobs, 1):
            print(f"\n  {i}. {job['title']}")
            print(f"     Company: {job['company_name']}")
            print(f"     Location: {job['location']}")
            print(f"     Similarity: {job['similarity']:.2%}")
            if job['salary_min'] and job['salary_max']:
                print(f"     Salary: ${job['salary_min']:,.0f} - ${job['salary_max']:,.0f}")
        
        print("\nResume matching test successful!")
        return True
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print()
    print("Embedding Service Test")
    print()
    
    # Basic tests
    result1 = test_embedding_service()
    
    # Resume search test (if basic tests pass)
    if result1:
        result2 = test_with_resume()
    else:
        result2 = False
        print("\nBasic tests failed, skipping resume search test")
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    print(f"  Basic tests: {'Passed' if result1 else 'Failed'}")
    print(f"  Resume search: {'Passed' if result2 else 'Failed/Skipped'}")
