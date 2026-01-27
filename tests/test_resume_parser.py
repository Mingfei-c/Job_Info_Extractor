"""
Test Resume Parser
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.resume_parser import ResumeParser, ResumeData


def test_parse_docx():
    """Test parsing DOCX resume"""
    
    resume_path = project_root / "resume" / "Mingfei Cao CV.docx"
    
    print("=" * 60)
    print("Test Resume Parser - DOCX")
    print("=" * 60)
    print(f"File path: {resume_path}")
    print()
    
    # Check if file exists
    if not resume_path.exists():
        print(f"Error: File not found - {resume_path}")
        return False
    
    try:
        # Parse resume
        resume = ResumeParser.parse(str(resume_path))
        
        print("Parse successful!")
        print()
        print(f"File type: {resume.file_type}")
        print(f"Characters: {resume.char_count}")
        print(f"Words: {resume.word_count}")
        print()
        
        # Show content preview
        print("=" * 60)
        print("[Resume Content Preview]")
        print("=" * 60)
        print()
        
        # Show first 3000 characters
        preview = resume.raw_text[:3000]
        print(preview)
        
        if len(resume.raw_text) > 3000:
            print()
            print(f"... ({len(resume.raw_text) - 3000} more characters)")
        
        print()
        print("=" * 60)
        print("Test passed!")
        return True
        
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print()
        print("Please install required dependencies:")
        print("  pip install python-docx pymupdf")
        return False
        
    except Exception as e:
        print(f"Parse failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_invalid_file():
    """Test invalid file handling"""
    
    print()
    print("=" * 60)
    print("Test Error Handling")
    print("=" * 60)
    
    # Test non-existent file
    try:
        ResumeParser.parse("not_exist.pdf")
        print("Should have raised FileNotFoundError")
        return False
    except FileNotFoundError:
        print("FileNotFoundError correctly raised")
    
    # Test unsupported format
    try:
        # Use an existing non-resume file for testing
        ResumeParser.parse(str(project_root / "pyproject.toml"))
        print("Should have raised ValueError")
        return False
    except ValueError as e:
        print(f"ValueError correctly raised: {e}")
    
    print()
    print("Error handling test passed!")
    return True


if __name__ == "__main__":
    print()
    print("Starting Resume Parser Tests")
    print()
    
    # Run tests
    result1 = test_parse_docx()
    result2 = test_invalid_file()
    
    print()
    print("=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    print(f"  DOCX parsing: {'Passed' if result1 else 'Failed'}")
    print(f"  Error handling: {'Passed' if result2 else 'Failed'}")
    print()
    
    if result1 and result2:
        print("All tests passed!")
    else:
        print("Some tests failed")
        sys.exit(1)
