import os
import re
import numpy as np
import pandas as pd
from loguru import logger

from sqlalchemy import text
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

DB_URL = (
    "postgresql+asyncpg://"
    f"{os.getenv('DB_ML_USER')}:{os.getenv('DB_ML_PASSWORD')}"
    f"@{os.getenv('DB_ML_HOST')}:{os.getenv('DB_ML_PORT')}/"
    f"{os.getenv('DB_ML_NAME')}"
)


async def create_db_table(query: str) -> str:
    """Create the table in a PostgreSQL database."""
    match = re.search(
        r'CREATE TABLE IF NOT EXISTS\s+(\w+)', query, re.IGNORECASE)
    if not match:
        raise ValueError("Query does not contain a valid statement.")

    table_name = match.group(1)

    engine = create_async_engine(DB_URL, echo=False)

    async with engine.begin() as conn:
        await conn.execute(text(query))  # ← оборачиваем строку в text()
        logger.debug(f'Executed create query on {table_name}')

    await engine.dispose()
    return table_name


async def check_table_exists(table_name: str) -> bool:
    """Check if a table exists in the PostgreSQL database.
    Args:
        DB_URL (str): PostgreSQL connection URL.
        table_name (str): Name of the table to check."""
    query = text(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        )
        """
    )

    engine = create_async_engine(DB_URL, echo=False)

    async with engine.connect() as conn:
        result = await conn.execute(query, {"table_name": table_name})
        exists = result.scalar()

    await engine.dispose()
    return exists


async def get_db_tables() -> list[str]:
    """Return a list of all user-defined tables in the PostgreSQL database."""
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
    """)

    engine = create_async_engine(DB_URL, echo=False)

    async with engine.connect() as conn:
        result = await conn.execute(query)
        tables = [row[0] for row in result.fetchall()]

    await engine.dispose()

    logger.info(f'Count tables from database: {len(tables)}')
    return tables


async def load_table_data(table_name: str) -> pd.DataFrame:
    """Load entire table from PostgreSQL database into a pandas DataFrame.
    Args:
        table_name (str): Name of the table in the PostgreSQL database."""
    query = f"SELECT * FROM {table_name};"

    # SQLAlchemy's async engine doesn't work directly with pandas
    # So we create a sync engine for just this one operation
    sync_url = URL.create(
        drivername="postgresql+psycopg",
        username=os.getenv('DB_ML_USER'),
        password=os.getenv('DB_ML_PASSWORD'),
        host=os.getenv('DB_ML_HOST'),
        port=os.getenv('DB_ML_PORT'),
        database=os.getenv('DB_ML_NAME'),
    )

    # Use synchronous engine to leverage pandas read_sql
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    logger.info(f'Loaded data from {table_name}: {df.shape}')
    return df


def generate_encode_table_create_query(column_name: str) -> str:
    """Generate SQL query to create dynamic encoding table
    for a given column."""
    return f'''
        CREATE TABLE IF NOT EXISTS encoding_{column_name} (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            original_value TEXT NOT NULL UNIQUE,
            encoded_value INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            run_timestamp TEXT NOT NULL
        );
    '''


async def insert_into_encode_table(
    table_name: str,
    records: list[tuple[str, int]],
    run_id: int,
    run_timestamp: str
) -> str:
    """Insert multiple encoded records into the specified encode table."""

    if not records:
        logger.warning(f"No records to insert into {table_name}")
        return table_name

    enriched_records = [
        {
            "original_value": original,
            "encoded_value": encoded,
            "run_id": run_id,
            "run_timestamp": run_timestamp
        }
        for original, encoded in records
    ]

    query = text(f"""
        INSERT INTO {table_name} (
            original_value, encoded_value, run_id, run_timestamp
        ) VALUES (
            :original_value, :encoded_value, :run_id, :run_timestamp
        );
    """)

    engine = create_async_engine(DB_URL, echo=False)

    async with engine.begin() as conn:
        for record in enriched_records:
            await conn.execute(query, record)

    await engine.dispose()

    logger.debug(f'Inserted {len(records)} encoded values into {table_name}')
    return table_name


async def init_encode_table(
    df_uniques: pd.DataFrame,
    column_name: str,
    run_id: int,
    run_timestamp: str
) -> pd.DataFrame:
    table_name = f"encoding_{column_name}"

    # Check if table exists
    if await check_table_exists(table_name):
        logger.debug(f'Table {table_name} already exists')
    else:
        create_query = generate_encode_table_create_query(column_name)
        await create_db_table(create_query)
        logger.debug(f'Created table {table_name}')

    # Load current data from encode table
    df_existing = await load_table_data(table_name)
    existing_values = set(df_existing['original_value'])
    logger.debug(f'Got existing encoded values: {len(existing_values)}')

    # Determine new values not yet encoded
    new_values = df_uniques[
        ~df_uniques['original_value'].isin(existing_values)
    ]['original_value'].tolist()
    logger.debug(f'Determined new values: {len(new_values)}')

    if not new_values:
        logger.debug('No new values to encode')
        return df_existing

    # Determine the next available encoding value
    max_value = df_existing['encoded_value'].max()
    next_code = int(max_value) + 1 if pd.notna(max_value) else 1
    logger.debug(f'Next starting code: {next_code}')

    # Build records to insert
    records = [(value, next_code + i) for i, value in enumerate(new_values)]
    logger.debug(f'Prepared records for insertion: {len(records)}')

    # Insert new records into encode_<column_name> table
    await insert_into_encode_table(
        table_name, records, run_id, run_timestamp
    )

    # Reload updated table
    df_updated = await load_table_data(table_name)
    return df_updated


# === combinations predictions
def generate_combinations_table_create_query(
        client_id: int,
        origin: str
) -> str:
    """Generate SQL query to create combinations table
    for a given client and origin.
    Args:
        client_id (int): Client identifier.
        origin (str): Origin name."""
    table_name = f'combinations_{client_id}_{origin}'
    return f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            client_id INTEGER,
            origin TEXT,
            upload_id INTEGER,
            city TEXT,
            profile TEXT,
            options INTEGER,
            options_2 INTEGER,
            options_3 INTEGER,
            callbacks INTEGER
        );'''


def check_combinations_upload_query(table_name: str, upload_id: int) -> str:
    """Build SQL query to fetch rows from combinations table by upload_id."""
    return f'''
        SELECT
            id, client_id, origin, upload_id, city, profile,
            options, options_2, options_3, callbacks
        FROM {table_name}
        WHERE upload_id = {upload_id};'''


def combinations_delete_query(table_name: str, upload_id: int) -> str:
    """Build a parameterized DELETE SQL statement by upload_id.
    Args:
        table_name (str): Target table name.
        upload_id (int): upload_id identifier."""
    query = f'DELETE FROM {table_name} WHERE upload_id = {upload_id}'
    return query


async def insert_into_combinations_table(
    client_id: int,
    origin: str,
    records: list[dict]
) -> str:
    """Insert multiple records into the combinations table
    for a given client and origin.
    Args:
        client_id (int): Client identifier.
        origin (str): Origin name.
        records (list[dict]): List of records with fields:
            upload_id (int), city (str), profile (str),
            options (int), options_2 (int), options_3 (int),
            callbacks (int)."""
    assert client_id > 0
    assert origin

    if not records:
        logger.warning(
            f'No records to insert into combinations_{client_id}_{origin}')
        return f'combinations_{client_id}_{origin}'

    table_name = f'combinations_{client_id}_{origin}'

    query = text(f"""
        INSERT INTO {table_name} (
            client_id, origin, upload_id, city, profile,
            options, options_2, options_3, callbacks
        ) VALUES (
            :client_id, :origin, :upload_id, :city, :profile,
            :options, :options_2, :options_3, :callbacks
        );
    """)

    enriched_records = [
        {
            'client_id': client_id,
            'origin': origin,
            'upload_id': rec['upload_id'],
            'city': rec['city'],
            'profile': rec['profile'],
            'options': rec['options'],
            'options_2': rec['options_2'],
            'options_3': rec['options_3'],
            'callbacks': rec['callbacks']
        }
        for rec in records
    ]

    engine = create_async_engine(DB_URL, echo=False)

    async with engine.begin() as conn:
        for record in enriched_records:
            await conn.execute(query, record)

    await engine.dispose()

    logger.debug(f'Inserted {len(records)} records into {table_name}')
    return table_name


async def init_combinations_table(
    df_records: pd.DataFrame,
    client_id: int,
    origin: str,
    upload_id: int
) -> pd.DataFrame:
    """Initialize combinations table for a given client and origin
    using upload_id-level rewrite.
    Args:
        df_records (pd.DataFrame):
            Records with columns 'upload_id', 'city', 'profile',
            'options', 'options_2', 'options_3', 'callbacks'.
        client_id (int): Client identifier.
        origin (str): Origin name."""
    assert client_id > 0
    assert upload_id > 0
    assert isinstance(origin, str) and origin.strip()
    assert isinstance(df_records, pd.DataFrame)

    required_cols = [
        'upload_id', 'city', 'profile',
        'options', 'options_2', 'options_3', 'callbacks']
    assert set(required_cols).issubset(df_records.columns)

    table_name = f'combinations_{client_id}_{origin}'

    if not await check_table_exists(table_name):
        create_query = generate_combinations_table_create_query(
            client_id, origin)
        await create_db_table(create_query)
        logger.debug(f'Created table {table_name}')

    # Check if table contains duplicates using upload_id
    engine = create_async_engine(DB_URL, echo=False)
    async with engine.connect() as conn:
        query = check_combinations_upload_query(table_name, int(upload_id))
        result = await conn.execute(text(query))
        rows = result.fetchall()
        cols_list = result.keys()
        df_existing = pd.DataFrame(rows, columns=cols_list)
    await engine.dispose()

    # Delete duplicates from table by upload_id
    if not df_existing.empty:
        delete_query = combinations_delete_query(table_name, int(upload_id))
        async with engine.begin() as conn_del:
            await conn_del.execute(
                text(delete_query), {'upload_id': int(upload_id)})
    await engine.dispose()

    # Convert DataFrame to list[dict] records for insertion
    df_to_insert = df_records[required_cols].copy()
    df_to_insert['upload_id'] = int(upload_id)
    df_to_insert = df_to_insert.where(pd.notna(df_to_insert), None)
    records = df_to_insert.to_dict(orient='records')

    # Insert into table
    await insert_into_combinations_table(
        client_id=client_id, origin=origin, records=records)
    logger.debug(f'Inserted {len(records)} records into {table_name}')
    return df_records


def generate_select_queries(df: pd.DataFrame) -> list[str]:
    """Build SELECT queries for combinations tables by row values.
    Args:
        df (pd.DataFrame): DataFrame with columns 'client_id', 'origin',
            'options', 'options_2', 'options_3', 'upload_id'."""
    required = [
        'client_id', 'origin', 'profile', 'city', 'options',
        'options_2', 'options_3', 'upload_id']

    if any(col not in df.columns for col in required):
        return []

    df_local = df[required].dropna(subset=required).copy()
    records = df_local.to_dict(orient='records')

    queries = []
    for rec in records:
        table_name = f"combinations_{
            int(rec['client_id'])}_{str(rec['origin'])}"
        query = (
            f'SELECT * FROM {table_name} '
            f'WHERE options = {int(rec["options"])} '
            f'AND options_2 = {int(rec["options_2"])} '
            f'AND options_3 = {int(rec["options_3"])} '
            f"AND profile = '{str(rec['profile'])}' "
            f"AND city = '{str(rec['city'])}' "
            f'AND upload_id = {int(rec["upload_id"])};'
        )
        queries.append(query)
    return queries


def round_options_step(df: pd.DataFrame) -> pd.DataFrame:
    """Round 'options_3' up to the nearest step depending on city,
    but only for rows where origin == 'av'.
    Expects columns: 'origin', 'city', 'options_3'."""
    df_out = df.copy()

    # no origin raises an error
    if 'origin' not in df_out.columns:
        raise KeyError("round_options_step expects column 'origin' in df")

    mask_av = df_out['origin'].astype(str).str.lower().eq('av')
    if not mask_av.any():
        return df_out  # нечего делать

    major_cities = ['москва', 'санкт-петербург']

    city_series = df_out.loc[mask_av, 'city'].astype(str).str.lower()

    steps = np.where(city_series.isin(major_cities), 2000, 300)

    values = (
        pd.to_numeric(df_out.loc[mask_av, 'options_3'], errors='coerce')
        .fillna(0)
        .astype(int)
        .to_numpy()
    )

    rounded = ((values + steps - 1) // steps) * steps
    df_out.loc[mask_av, 'options_3'] = rounded

    return df_out


async def init_custom_predict(
    df: pd.DataFrame
) -> tuple[bool, pd.DataFrame]:
    """Execute built SELECT query and attach callbacks to the input DataFrame.
    Args:
        df (pd.DataFrame): Input DataFrame to build query from and update."""
    # Add rounded step
    df = round_options_step(df)

    query = generate_select_queries(df)
    logger.info(f'QUERY: {query}')
    if not query:
        return False, df

    engine = create_async_engine(DB_URL, echo=False)
    async with engine.connect() as conn:
        result = await conn.execute(text(query[0]))
        rows = [dict(r) for r in result.mappings().all()]
        db_df = pd.DataFrame(rows)

    if db_df.empty:
        logger.error('No prediction pre-saved for this combination')
        return False, df

    callbacks_series = db_df['callbacks'].dropna()
    callbacks = callbacks_series.iloc[0]

    df = df.copy()
    df['callbacks'] = callbacks
    logger.success('Pre-saved prediction does exists')
    return True, df


# rv2: campaign
def generate_campaign_upload_table_create_query() -> str:
    """Generate SQL query for creating the campaign_upload table
    (if it does not exist)."""
    table_name = "campaign_upload"

    return f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            campaign_id TEXT,
            client_id INTEGER,
            origin TEXT,
            upload_id INTEGER
        );'''


async def init_campaign_upload(
    campaign_id: str,
    client_id: int,
    origin: str
) -> int:
    """Initialize campaign_upload entry"""
    table_name = "campaign_upload"

    # 1. Ensure table exists
    if not await check_table_exists(table_name):
        create_query = generate_campaign_upload_table_create_query()
        await create_db_table(create_query)
        logger.debug(f"Created table {table_name}")

    engine = create_async_engine(DB_URL, echo=False)

    # 2. Read max(upload_id)
    async with engine.connect() as conn:
        query_max = text(f"""
            SELECT MAX(upload_id) AS max_id
            FROM {table_name};
        """)
        result = await conn.execute(query_max)
        max_id = result.scalar()

    next_upload_id = (max_id + 1) if max_id is not None else 1
    logger.debug(f"Next upload_id = {next_upload_id}")

    # 3. Insert new entry
    insert_query = text(f"""
        INSERT INTO {table_name}
        (campaign_id, client_id, origin, upload_id)
        VALUES (:campaign_id, :client_id, :origin, :upload_id);
    """)

    async with engine.begin() as conn:
        await conn.execute(insert_query, {
            "campaign_id": campaign_id,
            "client_id": client_id,
            "origin": origin,
            "upload_id": next_upload_id
        })

    await engine.dispose()

    logger.info(
        f"Inserted campaign_upload entry → "
        f"(campaign_id={campaign_id}, client_id={client_id}, "
        f"origin={origin}, upload_id={next_upload_id})"
    )

    return next_upload_id


if __name__ == '__main__':
    import asyncio

    # # Custom Upload
    # client_id = 118
    # origin = 'av'
    # upload_id = 1

    # output_path = f'y/output_data/{client_id}/{origin}/'
    # file_path = f'combinations_{client_id}_{origin}.xlsx'
    # file_path = output_path + file_path

    # df = pd.read_excel(file_path)
    # # df = df[:10]
    # df['upload_id'] = upload_id

    # # output = asyncio.run(check_table_exists(table_name))
    # output = asyncio.run(
    #     init_combinations_table(df, client_id, origin, upload_id))
    # print(output)
    # # print(check_combinations_upload_query(client_id, upload_id))

    # Custom Predict
    client_id = 118
    origin = 'av'
    upload_id = 1

    output_path = f'y/output_data/{client_id}/{origin}/'
    file_path = f'custom_inputs_{client_id}_{origin}_preprocess.xlsx'
    # file_path = f'custom_inputs_{client_id}_{origin}_preprocess_test.xlsx'
    file_path = output_path + file_path

    df = pd.read_excel(file_path)
    # df = df[:10]
    df['upload_id'] = upload_id

    print(df[['options', 'options_2', 'options_3']])
    check, output = asyncio.run(init_custom_predict(df))
    print(output['callbacks'])
