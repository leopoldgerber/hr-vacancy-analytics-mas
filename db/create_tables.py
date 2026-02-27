import asyncio
import sqlparse
from loguru import logger
from importlib import resources

from db.scripts.credentials import DB_URL
from db.scripts.connection import execute_query


def load_sql_file(dir_name: str, file_name: str) -> str:
    """Load SQL file content from a package directory.
    Args:
        dir_name (str): Package-like directory path (e.g. 'db/create').
        file_name (str): SQL file name."""
    dir_name = dir_name.replace('/', '.')

    sql_path = resources.files(dir_name).joinpath(file_name)
    with sql_path.open('r', encoding='utf-8') as file:
        return file.read()


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL script into statements safely.
    Args:
        sql (str): SQL script content."""
    if not isinstance(sql, str) or not sql.strip():
        return []

    statements = []
    for statement in sqlparse.split(sql):
        statement = statement.strip()
        if statement:
            statements.append(statement)
    return statements


async def run_script(dir_name: str, file_name: str) -> int:
    """Execute SQL script statement-by-statement in one transaction.
    Args:
        dir_name (str): Directory name with SQL scripts.
        file_name (str): SQL script file name."""
    # Load SQL script file
    sql_script = load_sql_file(dir_name, file_name)
    # Parse statements in SQL script file
    statements = split_sql_statements(sql_script)

    logger.info(
        f'Loaded sql script: {file_name}. '
        f'Statements to execute: {len(statements)}')

    # Execute parsed statements
    executed_count = await execute_query(
        statements=statements,
        db_url=DB_URL['vacancy-postgres'],
        raise_on_error=True,
    )

    logger.info(
        f'SQL script {file_name} executed successfully. '
        f'Executed statements: {executed_count}')

    return executed_count


async def run_init_script() -> list[int]:
    """Execute SQL queries for table initialization.
    Create table and insert data.
    Args:
        None."""
    dir_list = [
        'db/create',
        'db/insert'
    ]
    file_list = [
        '001_create_vacancy_activity.sql',
        '001_insert_vacancy_activity.sql',
    ]

    executed_list = []
    for dir_name, file_name in zip(dir_list, file_list):
        logger.info(f'Start: {dir_name} → {file_name}')
        executed_count = await run_script(dir_name, file_name)
        executed_list.append(executed_count)
        logger.info(f'Finished: {dir_name} → {file_name}')

    return executed_list


if __name__ == '__main__':
    executed_counts = asyncio.run(run_init_script())
    logger.info(f'Executed counts per file: {executed_counts}')
