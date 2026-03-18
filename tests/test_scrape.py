"""
Test fetching full descriptions from Adzuna job pages - debug version
"""

import requests
from bs4 import BeautifulSoup


def get_full_description(url: str) -> dict:
    """Fetch full job description from an Adzuna job page"""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    # Save raw HTML for debugging
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"Saved raw HTML to debug_page.html ({len(response.text)} chars)")

    soup = BeautifulSoup(response.text, "html.parser")

    # Get title
    title = ""
    title_tag = soup.find("h1")
    if title_tag:
        title = title_tag.get_text(strip=True)

    # Try multiple methods to extract description
    description = ""

    # Method 1: find divs with "description" in class name
    desc_divs = soup.find_all("div", class_=True)
    for div in desc_divs:
        classes = " ".join(div.get("class") or [])
        if "description" in classes.lower():
            text = div.get_text(separator="\n", strip=True)
            if len(text) > len(description):
                description = text
                print(f"Method 1 found: class='{classes}', length={len(text)}")

    # Method 2: find section elements
    if not description:
        sections = soup.find_all("section")
        for section in sections:
            text = section.get_text(separator="\n", strip=True)
            if len(text) > 200:
                print(f"Method 2 found: section, length={len(text)}")
                description = text
                break

    # Method 3: find article elements
    if not description:
        articles = soup.find_all("article")
        for article in articles:
            text = article.get_text(separator="\n", strip=True)
            if len(text) > 200:
                print(f"Method 3 found: article, length={len(text)}")
                description = text
                break

    # Method 4: find main content area
    if not description:
        main = soup.find("main") or soup.find("div", {"role": "main"})
        if main:
            description = main.get_text(separator="\n", strip=True)
            print(f"Method 4 found: main, length={len(description)}")

    # Clean description: remove EEO statement and everything after it
    description = clean_description(description)

    return {"title": title, "description": description, "url": url}


def clean_description(text: str) -> str:
    """
    Clean job description by removing EEO statement and all content that follows
    """
    if not text:
        return text

    # Truncate at these keywords (case-insensitive)
    cutoff_keywords = [
        "eeo statement",
        "equal employment opportunity",
        "equal opportunity employer",
        "we are an equal opportunity",
        "e-verify",
        "eeoc",
        "affirmative action",
        "discrimination policy",
    ]

    text_lower = text.lower()

    # Find the earliest occurring cutoff keyword
    earliest_idx = len(text)
    for keyword in cutoff_keywords:
        idx = text_lower.find(keyword)
        if idx != -1 and idx < earliest_idx:
            earliest_idx = idx

    if earliest_idx < len(text):
        cleaned = text[:earliest_idx].strip()
        print(f"Cleaned EEO content: {len(text)} -> {len(cleaned)} chars")
        return cleaned

    return text


if __name__ == "__main__":
    test_url = "https://www.adzuna.com/search?q=software&w=10008"

    print(f"Fetching: {test_url}\n")

    try:
        result = get_full_description(test_url)

        print("\n" + "=" * 60)
        print(f"Title: {result['title']}")
        print("=" * 60)

        # Save to txt file
        with open("extracted_text.txt", "w", encoding="utf-8") as f:
            f.write(f"URL: {result['url']}\n")
            f.write(f"Title: {result['title']}\n")
            f.write("=" * 60 + "\n")
            f.write("Description:\n")
            f.write(result["description"] if result["description"] else "Description not found")

        print("\nSaved to extracted_text.txt")

        if result["description"]:
            print(f"Description length: {len(result['description'])} chars")
        else:
            print("\nDescription not found!")
            print("Please inspect debug_page.html to analyze page structure")

    except requests.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"Parse error: {e}")
        import traceback

        traceback.print_exc()
