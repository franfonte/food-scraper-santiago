from dotenv import load_dotenv
import os
from supabase import create_client, Client

load_dotenv()  # carga variables del archivo .env

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(url, key)
print("✅ Conexión a Supabase exitosa!")