import pymongo
from urllib.parse import quote_plus

MONGO_USER = quote_plus("labbuild_user")
MONGO_PASSWORD = quote_plus("$$u1QBd6&372#$rF")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/labbuild_db"