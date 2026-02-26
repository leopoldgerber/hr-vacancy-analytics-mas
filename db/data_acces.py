import pandas as pd
from loguru import logger
from datetime import datetime, timedelta

from scripts.credentials import DB_URL
from scripts.connection import execute_query

