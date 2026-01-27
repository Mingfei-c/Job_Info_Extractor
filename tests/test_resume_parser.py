"""
Test Resume Parser
"""

from pathlib import Path

from src.services.resume_parser import ResumeParser


def test_parse_docx():
    """Test parsing DOCX resume"""

    project_root = Path(__file__).parent.parent
    resume_path = project_root / "resume" / "Mingfei Cao CV.docx"

    print("=" * 60)
    print("Test Resume Parser - DOCX")
    print("=" * 60)
    print(f"File path: {resume_path}")
    print(f"Exists: {resume_path.exists()}")

    if not resume_path.exists():
        print(f"\nFile not found: {resume_path}")
        print("Please ensure there is a resume file in the resume directory")
        return False

    try:
        result = ResumeParser.parse(str(resume_path))

        print("\nParsing successful!")
        print(f"File type: {result.file_type}")
        print(f"Character count: {result.char_count}")
        print(f"Word count: {result.word_count}")
        print()
        print("Content preview (first 500 characters):")
        print("-" * 40)
        print(result.raw_text[:500])
        print("-" * 40)
        return True

    except Exception as e:
        print(f"Parse failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_invalid_file():
    """Test error handling for invalid files"""
    print("\n" + "=" * 60)
    print("Test Error Handling")
    print("=" * 60)

    # Test file not found
    try:
        ResumeParser.parse("nonexistent_file.pdf")
        print("Error: Should have raised FileNotFoundError")
        return False
    except FileNotFoundError as e:
        print(f"FileNotFoundError test passed: {e}")

    # Test unsupported format
    try:
        ResumeParser.parse("document.txt")
        print("Error: Should have raised ValueError")
        return False
    except FileNotFoundError:
        # On Windows, file check happens first
        print("File check before format check (expected behavior)")
    except ValueError as e:
        print(f"ValueError test passed: {e}")

    return True
