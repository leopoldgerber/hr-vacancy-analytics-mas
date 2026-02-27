# import pandas as pd
from loguru import logger
# from datetime import datetime, timedelta

from db.scripts.credentials import DB_URL
from db.scripts.connection import execute_query_return_df


print(DB_URL['vacancy-postgres'])


async def test_db_connection() -> bool:
    """Check database connectivity using execute_query().
    Returns:
        bool: True if connection is successful, otherwise False."""
    query = 'SELECT 1 AS healthcheck'

    try:
        df = await execute_query(
            query=query,
            db_url=DB_URL['vacancy-postgres'])

        is_connected = not df.empty and int(df.iat[0, 0]) == 1

        if is_connected:
            logger.info('Database connection test succeeded.')
        else:
            logger.error(
                'Database connection test failed: unexpected response.')
        return is_connected

    except Exception as error:
        logger.error(f'Database connection test failed: {error}')
        return False


if __name__ == '__main__':
    import asyncio

    # Check connection
    is_connected = asyncio.run(test_db_connection())
    logger.info(f'Is connected: {is_connected}')
