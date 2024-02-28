# from pydantic import BaseSettings
from pydantic import BaseConfig


class APISettings(BaseConfig):
    max_wait_time_count: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
