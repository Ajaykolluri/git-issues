from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    github_token: str = Field(validation_alias="GITHUB_TOKEN")

    model_config = SettingsConfigDict(case_sensitive=True)

