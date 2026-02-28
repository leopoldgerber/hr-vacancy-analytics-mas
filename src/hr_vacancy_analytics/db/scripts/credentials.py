import os
from dotenv import load_dotenv
load_dotenv()

local_user = "myuser"
local_pass = "mypassword"
local_host = "localhost"
local_port = 5432
local_db = "mydatabase"


def build_connection_url(
        user: str, password: str, host: str, port: int, name: str) -> str:
    """Build async PostgreSQL connection URL.
    Args:
        user (str): Database user name.
        password (str): Database user password.
        host (str): Database host.
        port (int): Database port.
        name (str): Database name."""
    return (
        'postgresql+asyncpg://'
        f'{user}:{password}@{host}:{port}/{name}')


DB_URL = {
    "vacancy-postgres": build_connection_url(
        os.getenv("POSTGRES_USER", local_user),
        os.getenv("POSTGRES_PASSWORD", local_pass),
        os.getenv("POSTGRES_HOST", local_host),
        os.getenv("POSTGRES_PORT", local_port),
        os.getenv("POSTGRES_DB", local_db)
    ),
}
