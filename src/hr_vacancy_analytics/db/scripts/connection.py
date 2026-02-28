import pandas as pd
from typing import Any
from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_async_engine_instance(db_url: str) -> AsyncEngine:
    """Create asynchronous SQLAlchemy engine.
    Args:
        db_url (str): Database connection URL."""
    return create_async_engine(db_url, echo=False, future=True)


async def check_async_connection(engine: AsyncEngine) -> Any:
    """Check database connection with test query.
    Args:
        engine (AsyncEngine): Async SQLAlchemy engine."""
    async with engine.connect() as connection:
        result = await connection.execute(text('SELECT 1'))
        row = result.fetchone()
    return row


async def execute_query(
    statements: list[str],
    db_url: str,
    raise_on_error: bool = False,
) -> int:
    """Execute multiple SQL statements in a single transaction.
    Args:
        statements (list[str]): List of SQL statements to execute.
        db_url (str): Database URL.
        raise_on_error (bool): Raise exception on execution error."""
    if not isinstance(statements, list) or not statements:
        logger.error('Statements must be a non-empty list.')
        return 0

    engine = create_async_engine_instance(db_url)
    executed_count = 0

    try:
        async with engine.begin() as connection:
            for statement in statements:
                if not isinstance(statement, str) or not statement.strip():
                    continue
                await connection.execute(text(statement))
                executed_count += 1

        logger.info(f'Executed statements: {executed_count}')
        return executed_count

    except Exception as error:
        logger.error(f'Error executing SQL statements: {error}')
        if raise_on_error:
            raise
        return executed_count

    finally:
        await engine.dispose()


async def execute_query_return_df(
    query: str,
    db_url: str,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Execute a SQL query asynchronously and return a pandas DataFrame.
    Args:
        query (str): SQL query string that may include named parameters.
        db_url (str): Database URL for async engine creation.
        params (dict[str, Any] | None): Optional parameters."""
    if not isinstance(query, str) or not query.strip():
        logger.error('Query must be a non-empty string.')
        return pd.DataFrame()

    engine = create_async_engine(db_url, future=True)
    df = pd.DataFrame()
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(query), params or {})
            columns = list(result.keys())
            rows = result.mappings().all()
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f'Loaded df shape: {df.shape}')
    except Exception as error:
        logger.error(f'Error executing query: {error}')
    finally:
        await engine.dispose()
    return df
