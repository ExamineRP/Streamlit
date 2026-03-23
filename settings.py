import os

# PostgreSQL 데이터베이스 연결 설정
DB_HOST = os.getenv("DB_HOST", "10.206.103.174")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "kbam")