from pathlib import Path
import configparser


from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


file_config = Path(__file__).parent.parent.joinpath("config.ini")
config = configparser.ConfigParser()
config.read(file_config)


user = config.get("DEV_DB", "USER")
password = config.get("DEV_DB", "PASSWORD")
domain = config.get("DEV_DB", "DOMAIN")
port = config.get("DEV_DB", "PORT")
db = config.get("DEV_DB", "DB_NAME")


URI = f"postgresql://{user}:{password}@{domain}:{port}/{db}"


engine = create_engine(URI, pool_size=5, max_overflow=0)
DBSession = sessionmaker(bind=engine)
session = DBSession()
