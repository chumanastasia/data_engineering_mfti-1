from pydantic import PositiveInt
from pydantic_settings import BaseSettings
from yarl import URL


class DSNConfig(BaseSettings):
    ncbi_dsn: URL = URL("https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi")


class LuigiConfig(BaseSettings):
    max_workers: PositiveInt = 1
    local_scheduler: bool = True
    no_lock: bool = False
