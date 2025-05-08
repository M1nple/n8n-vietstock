from sqlalchemy import create_engine
import psycopg2

# db_url = "postgresql://postgres:652003@172.17.0.1:5432/vnstock" 
db_url = "postgresql://postgres:652003@host.docker.internal:5432/vnstock"
engine = create_engine(db_url)

try:
    with engine.connect() as conn:
        print("✅ Kết nối DB thành công")
except Exception as e:
    print("❌ Lỗi kết nối DB:", e)
