import os

DB_URL = os.getenv("DB_URL", "sqlite://:memory:")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
