"""
测试从 Adzuna 职位页面获取完整描述 - 调试版本
"""

import requests
from bs4 import BeautifulSoup


def get_full_description(url: str) -> dict:
    """从 Adzuna 职位页面获取完整描述"""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    # 保存原始 HTML 用于调试
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"已保存原始 HTML 到 debug_page.html ({len(response.text)} 字符)")

    soup = BeautifulSoup(response.text, "html.parser")

    # 获取标题
    title = ""
    title_tag = soup.find("h1")
    if title_tag:
        title = title_tag.get_text(strip=True)

    # 尝试多种方式获取描述
    description = ""

    # 方法1: 查找包含 "description" 的 class
    desc_divs = soup.find_all("div", class_=True)
    for div in desc_divs:
        classes = " ".join(div.get("class", []))
        if "description" in classes.lower():
            text = div.get_text(separator="\n", strip=True)
            if len(text) > len(description):
                description = text
                print(f"方法1找到: class='{classes}', 长度={len(text)}")

    # 方法2: 查找 section 元素
    if not description:
        sections = soup.find_all("section")
        for section in sections:
            text = section.get_text(separator="\n", strip=True)
            if len(text) > 200:
                print(f"方法2找到: section, 长度={len(text)}")
                description = text
                break

    # 方法3: 查找 article 元素
    if not description:
        articles = soup.find_all("article")
        for article in articles:
            text = article.get_text(separator="\n", strip=True)
            if len(text) > 200:
                print(f"方法3找到: article, 长度={len(text)}")
                description = text
                break

    # 方法4: 查找主要内容区域
    if not description:
        main = soup.find("main") or soup.find("div", {"role": "main"})
        if main:
            description = main.get_text(separator="\n", strip=True)
            print(f"方法4找到: main, 长度={len(description)}")

    # 清理描述：删除 EEO statement 及之后的内容
    description = clean_description(description)

    return {"title": title, "description": description, "url": url}


def clean_description(text: str) -> str:
    """
    清理职位描述，删除 EEO statement 及之后的所有内容
    """
    if not text:
        return text

    # 在这些关键词处截断（不区分大小写）
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

    # 找到最早出现的截断关键词
    earliest_idx = len(text)
    for keyword in cutoff_keywords:
        idx = text_lower.find(keyword)
        if idx != -1 and idx < earliest_idx:
            earliest_idx = idx

    if earliest_idx < len(text):
        cleaned = text[:earliest_idx].strip()
        print(f"已清理 EEO 内容: {len(text)} -> {len(cleaned)} 字符")
        return cleaned

    return text


if __name__ == "__main__":
    test_url = "https://www.adzuna.com/search?q=software&w=10008"

    print(f"正在获取: {test_url}\n")

    try:
        result = get_full_description(test_url)

        print("\n" + "=" * 60)
        print(f"标题: {result['title']}")
        print("=" * 60)

        # 保存到 txt 文件
        with open("extracted_text.txt", "w", encoding="utf-8") as f:
            f.write(f"URL: {result['url']}\n")
            f.write(f"标题: {result['title']}\n")
            f.write("=" * 60 + "\n")
            f.write("描述:\n")
            f.write(result["description"] if result["description"] else "未能找到描述")

        print("\n已保存到 extracted_text.txt")

        if result["description"]:
            print(f"描述长度: {len(result['description'])} 字符")
        else:
            print("\n未能找到描述！")
            print("请查看 debug_page.html 分析页面结构")

    except requests.RequestException as e:
        print(f"请求错误: {e}")
    except Exception as e:
        print(f"解析错误: {e}")
        import traceback

        traceback.print_exc()
