from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    OPENAQ_ENV: str = "staging"
    DATABASE_READ_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_USER: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_DB: str
    IP_ADDRESS: str
    KEY_NAME: str

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        elif 'ENV' in environ:
            env_file = Path.joinpath(parent, f".env.{environ['ENV']}")
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
