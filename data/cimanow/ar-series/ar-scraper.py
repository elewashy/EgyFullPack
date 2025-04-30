# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import re

# Simple Arabic to Franco mapping (expand as needed)
AR_FRANCO = {
    "ا": "a", "أ": "a", "إ": "e", "آ": "aa", "ب": "b", "ت": "t", "ث": "th", "ج": "g", "ح": "7",
    "خ": "kh", "د": "d", "ذ": "z", "ر": "r", "ز": "z", "س": "s", "ش": "sh", "ص": "s", "ض": "d",
    "ط": "t", "ظ": "z", "ع": "3", "غ": "gh", "ف": "f", "ق": "2", "ك": "k", "ل": "l", "م": "m",
    "ن": "n", "ه": "h", "و": "w", "ي": "y", "ى": "a", "ء": "2", "ة": "a", "ئ": "2", "ؤ": "2",
    " ": " ", "ـ": "", "ً": "", "ٌ": "", "ٍ": "", "َ": "", "ُ": "", "ِ": "", "ّ": "", "ْ": ""
}

def arabic_to_franco(text):
    return ''.join(AR_FRANCO.get(c, c) for c in text)

def extract_season(text):
    # Extract season number from Arabic text like "الموسم الثاني" or "الموسم 02"
    match = re.search(r"الموسم\s*(\d+|الأول|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع|العاشر)", text)
    if match:
        val = match.group(1)
        arabic_nums = {
            "الأول": 1, "الثاني": 2, "الثالث": 3, "الرابع": 4, "الخامس": 5,
            "السادس": 6, "السابع": 7, "الثامن": 8, "التاسع": 9, "العاشر": 10
        }
        if val.isdigit():
            return f"S{int(val):02d}"
        elif val in arabic_nums:
            return f"S{arabic_nums[val]:02d}"
    return "S01"

def clean_title(title):
    # Remove genre if present in <em>...</em>
    return re.sub(r"<em>.*?</em>", "", title).strip()

def main():
    base_url = "https://cimanow.cc/category/%D9%85%D8%B3%D9%84%D8%B3%D9%84%D8%A7%D8%AA-%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9/"
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []
    page = 1
    while True:
        if page == 1:
            url = base_url
        else:
            url = base_url + f"page/{page}/"
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.find_all("article", {"aria-label": "post"})
        if not articles:
            break
        for article in articles:
            a_tag = article.find("a", href=True)
            link = a_tag["href"] if a_tag else ""
            img_tag = article.find("img", class_="lazy")
            if img_tag:
                image = img_tag.get("data-src") or img_tag.get("src") or ""
            else:
                image = ""
            info_ul = article.find("ul", class_="info")
            ribbons = article.find_all("li", {"aria-label": "ribbon"})
            ribbon_value = [li.text.strip() for li in ribbons]
            year = ""
            for li in article.find_all("li", {"aria-label": "year"}):
                year = li.text.strip()
            # Season extraction
            season = "S01"
            for li in info_ul.find_all("li", {"aria-label": "tab"}):
                if "موسم" in li.text:
                    season = extract_season(li.text)
            for li in ribbons:
                if "موسم" in li.text:
                    season = extract_season(li.text)
            # Title and genre
            title_li = info_ul.find("li", {"aria-label": "title"})
            if title_li:
                # Extract only the text (without <em>), and genre from <em>
                em = title_li.find("em")
                genre = em.text.strip() if em else ""
                # Remove <em> from the text and strip
                title_text = title_li.get_text(separator=" ", strip=True)
                if em:
                    title = title_text.replace(genre, "").strip(" ،,")
                else:
                    title = title_text
            else:
                title = ""
                genre = ""
            # Franco name
            franco_name = "[EgyFilm] " + arabic_to_franco(title) + f" {season}"
            results.append({
                "name": franco_name,
                "title_ar": title,
                "genre": genre,
                "season": season,
                "year": year,
                "image": image,
                "link": link,
                "ribbon": ribbon_value
            })
        page += 1

    # Assign descending IDs
    total = len(results)
    for idx, item in enumerate(results):
        item["id"] = total - idx

    with open("data/cimanow/ar-series/ar-series.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
