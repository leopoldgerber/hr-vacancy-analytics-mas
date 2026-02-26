import asyncio
from importlib import resources
from typing import List

from loguru import logger

from db.scripts.credentials import DB_URL
from db.scripts.connection import execute_initial_query


def load_sql_file(filename: str) -> str:
    """Load SQL file content from db/init directory."""
    sql_path = resources.files('db.init').joinpath(filename)
    with sql_path.open('r', encoding='utf-8') as file:
        return file.read()


def split_sql_statements(sql: str) -> List[str]:
    """Split SQL script into separate statements by semicolon."""
    statements = []
    for chunk in sql.split(';'):
        statement = chunk.strip()
        if statement:
            statements.append(statement)
    return statements


async def run_init_script(filename: str) -> None:
    """Execute SQL init script statement-by-statement."""
    sql_script = load_sql_file(filename)
    statements = split_sql_statements(sql_script)

    logger.info(
        f'Loaded init script: {filename}. '
        f'Statements to execute: {len(statements)}'
    )

    for idx, statement in enumerate(statements, start=1):
        try:
            await execute_initial_query(
                query=statement,
                db_url=DB_URL['vacancy-postgres']
            )
            logger.info(f'Executed statement {idx}/{len(statements)}')
        except Exception as error:
            logger.error(
                f'Failed at statement {idx}/{len(statements)}: {error}'
            )
            logger.error(f'Failed SQL:\n{statement}')
            raise

    logger.info(f'Init script {filename} executed successfully.')


if __name__ == '__main__':
    asyncio.run(run_init_script('001_create_vacancy_activity.sql'))
