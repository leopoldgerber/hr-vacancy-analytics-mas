import os
import pandas as pd
from typing import Any
from loguru import logger
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from dotenv import load_dotenv
load_dotenv()

DB_URL = (
    "postgresql+asyncpg://"
    f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)


async def execute_query(
    query: str,
    params: dict[str, Any] | None = None
) -> pd.DataFrame:
    """Execute a SQL query asynchronously and return a pandas DataFrame.
    Args:
        query (str):
            SQL query string that may include
            named parameters like ':table_name'.
        params (dict[str, Any] | None):
            Optional parameters for bound placeholders."""
    assert isinstance(query, str) and query.strip() != ''
    engine = create_async_engine(DB_URL, future=True)

    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(query), params or {})
            columns = list(result.keys())
            rows = result.mappings().all()
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f'Loaded df shape: {df.shape}')
    except Exception as e:
        logger.error(f'Error executing query: {e}')
        df = pd.DataFrame()
    finally:
        await engine.dispose()
    return df


def get_dates_from_prediction_date(prediction_date: str) -> dict[str, str]:
    """Get date range for the last eight weeks before.
    Args:
        prediction_date (str): Date in format 'YYYY-MM-DD'."""
    dt = date.fromisoformat(prediction_date)
    week_start = dt - timedelta(days=dt.isoweekday() - 1)
    from_date = (week_start - timedelta(weeks=8)).isoformat()
    to_date = week_start.isoformat()
    return {'date_from': from_date, 'date_to': to_date}


# --- QUERIES
async def get_plans_data(client_id: int, upload_id: int) -> str:
    query = f"""
        SELECT
            p.client_id
            , p.upload AS upload_id
            , p.real_profile AS profile
            , p.real_location AS city
            , COUNT(DISTINCT p.object_id) AS objects_count
        FROM
            plans AS p
        WHERE 1=1
            AND p.client_id = {client_id}
            AND p.upload = {upload_id}
        GROUP BY
            p.client_id
            , p.upload
            , p.real_profile
            , p.real_location"""
    return await execute_query(query)


async def get_hv_weekly_options(
        client_id: int,
        prediction_date: str
) -> str:
    dates_dict = get_dates_from_prediction_date(prediction_date)
    date_from = dates_dict['date_from']
    date_to = dates_dict['date_to']

    query = f"""
        SELECT
            t_lines.client_id
          , t_lines.origin
          , t_lines.city
          , t_lines."profile"
          , t_lines.week_year
          , SUM(t_lines.options)   AS OPTIONS
          , SUM(t_lines.options_2) AS options_2
          , SUM(t_lines.options_3) AS options_3
        FROM
            (SELECT
                lines.client_id
              , 'hv' AS origin
              , lines.region   AS city
              , lines."profile"
              , lines.date_from
              , to_char(lines.date_from::date, 'IW')::int AS week_year
              , lines.publications_standart         AS options
              , lines.publications_standart_plus    AS options_2
              , lines.publications_standart_premium AS options_3
            FROM hv_lines AS lines
            WHERE 1=1
              AND lines.client_id = {client_id}
              AND lines.days < 11
              AND LOWER(lines."profile") <> 'другое'
              AND lines.date_from >= '{date_from}'
              AND lines.date_to < '{date_to}'
            ) AS t_lines
        GROUP BY
            t_lines.client_id
          , t_lines.origin
          , t_lines.city
          , t_lines."profile"
          , t_lines.week_year
    """
    return await execute_query(query)


async def get_av_weekly_options(client_id: int, prediction_date: str) -> str:
    """Get weekly options aggregation for 'av' source.
    Args:
        client_id (int): Client identifier.
        prediction_date (str): Date in format 'YYYY-MM-DD'."""
    dates_dict = get_dates_from_prediction_date(prediction_date)
    date_from = dates_dict['date_from']
    date_to = dates_dict['date_to']

    query = f"""
        SELECT
            t_lines.client_id
          , t_lines.origin
          , t_lines.city
          , t_lines."profile"
          , t_lines.week_year
          , SUM(t_lines.options)   AS OPTIONS
          , SUM(t_lines.options_2) AS options_2
          , SUM(t_lines.options_3) AS options_3
        FROM
            (SELECT
                lines.client_id
              , 'av' AS origin
              , lines.region   AS city
              , lines."profile"
              , lines.date_from
              , to_char(lines.date_from::date, 'IW')::int AS week_year
              , lines.publication       AS options
              , 0                        AS options_2
              , lines.services_price     AS options_3
            FROM av_lines AS lines
            WHERE 1=1
              AND lines.client_id = {client_id}
              AND lines.days < 11
              AND LOWER(lines."profile") <> 'другое'
              AND lines.date_from >= '{date_from}'
              AND lines.date_to < '{date_to}'
            ) AS t_lines
        GROUP BY
            t_lines.client_id
          , t_lines.origin
          , t_lines.city
          , t_lines."profile"
          , t_lines.week_year
    """
    return await execute_query(query)


async def get_weekly_median(
        client_id: int,
        origin: str,
        prediction_date: str
) -> pd.DataFrame:
    """Compute median options per client, origin, city, profile, and ISO week.
    Args:
        client_id (int): Client identifier.
        origin (str): Data source, 'av' or 'hv'.
        prediction_date (str): Date in format 'YYYY-MM-DD'."""
    if origin == 'av':
        df = await get_av_weekly_options(client_id, prediction_date)
    elif origin == 'hv':
        df = await get_hv_weekly_options(client_id, prediction_date)
    else:
        raise ValueError(f'Invalid origin: {origin}. Expected "av" or "hv"')

    group_cols = ['client_id', 'origin', 'city', 'profile']
    value_cols = ['options', 'options_2', 'options_3']

    df = (
        df[group_cols + value_cols]
        .groupby(group_cols, dropna=False)
        .median(numeric_only=True)
        .reset_index()
        .rename(columns={
            'options': 'median_options',
            'options_2': 'median_options_2',
            'options_3': 'median_options_3',
        })
    )
    return df


if __name__ == '__main__':
    import asyncio

    client_id = 132
    origin = 'hv'
    upload_id = 434
    prediction_date = '2025-09-22'

    # output_path = f'y/output_data/{client_id}/{origin}/'
    # file_path = f'combinations_{client_id}_{origin}.xlsx'
    # file_path = output_path + file_path

    # df = pd.read_excel(file_path)
    # df = df[:10]
    # df['upload_id'] = upload_id

    # output = asyncio.run(get_plans_data(client_id, upload_id))
    # print(output.shape)

    output = asyncio.run(get_hv_weekly_options(client_id, prediction_date))
    print(output.shape)

    output = asyncio.run(get_av_weekly_options(client_id, prediction_date))
    print(output.shape)

    output = asyncio.run(get_weekly_median(client_id, origin, prediction_date))
    print(output.shape)

    output.to_excel('output_weekly_lines.xlsx', index=False)
