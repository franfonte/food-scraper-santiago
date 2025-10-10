import requests, json, re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv()

def fetch_ubereats_json(zone_url):
    headers = {"User-Agent": os.getenv("USER_AGENT")}
    r = requests.get(zone_url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    match = re.search(r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});", r.text)
    if not match:
        raise ValueError("Could not find JSON")
    data = json.loads(match.group(1))
    return data

def scrape_zone(zone):
    url = f"https://www.ubereats.com/cl/feed?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNhbS4lMjBTYW4lMjBGcmFuY2lzY28lMjBkZSUyMEFzaXMlMjAxNzAwJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0ElMjIzMy4{zone['latitude']}JTIyJTJDJTIybG9uZ2l0dWRlJTIyJTNBLTcwLjUwMTc4NTA5OTk5OTk5JTdE"
    data = fetch_ubereats_json(url)
    # later: parse restaurant + menu data
    return data
