from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

def get_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

def upload_data(client, restaurants, foods):
    if restaurants:
        client.table(os.getenv("SUPABASE_TABLE_RESTAURANTS")).upsert(restaurants).execute()
    if foods:
        client.table(os.getenv("SUPABASE_TABLE_FOOD_ITEMS")).upsert(foods).execute()
