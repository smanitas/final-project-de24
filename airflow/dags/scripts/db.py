from sqlmodel import create_engine
from config import CONFIG


class Database:
    def __init__(self):
        db_config = CONFIG.get_database_config()
        self.user = db_config['user']
        self.password = db_config['password']
        self.host = db_config['host']
        self.port = db_config['port']
        self.database = db_config['database']
        self.engine = self.create_engine()

    def create_engine(self):
        url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(url, echo=False)
