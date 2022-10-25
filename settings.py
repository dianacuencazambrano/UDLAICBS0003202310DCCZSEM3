from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class Settings:
    DB_TYPE: str
    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_STG: str
    DB_SOR: str

load_dotenv()

settings = Settings(
            DB_TYPE=os.getenv('DB_TYPE'),
            DB_HOST=os.getenv('DB_HOST'),
            DB_PORT=os.getenv('DB_PORT'),
            DB_USER=os.getenv('DB_USER'),
            DB_PASSWORD=os.getenv('DB_PASSWORD'),
            DB_STG=os.getenv('DB_STG'),
            DB_SOR=os.getenv('DB_SOR'),
        )

