import sys
import os
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

class Config:
    DB_HOST     = os.getenv("DB_HOST")
    DB_PORT     = int(os.getenv("DB_PORT", 5432))
    DB_NAME     = os.getenv("DB_NAME")
    DB_USER     = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_SSLMODE  = os.getenv("DB_SSLMODE", "disable")

    DEBUG   = False
    TESTING = False

    DEFAULT_PAGE_SIZE = 50
    MAX_PAGE_SIZE     = 500


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


config_map = {
    "development": DevelopmentConfig,
    "production":  ProductionConfig,
}