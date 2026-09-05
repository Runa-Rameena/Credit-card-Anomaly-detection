import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))

SNOWFLAKE_CONFIG = {
    'account':   os.environ.get('SNOWFLAKE_ACCOUNT'),
    'user':      os.environ.get('SNOWFLAKE_USER'),
    'password':  os.environ.get('SNOWFLAKE_PASSWORD'),
    'database':  os.environ.get('SNOWFLAKE_DATABASE'),
    'schema':    os.environ.get('SNOWFLAKE_SCHEMA'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
}

class SnowflakeWriter:

    def __init__(self):
        self.conn = None
        self._connect()

    def _connect(self):
        try:
            self.conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logger.info('Connected to Snowflake')
        except Exception as e:
            logger.error(f'Snowflake connection failed: {e}')
            raise

    def write_batch(self, transactions: list) -> int:
        if not transactions:
            return 0
        try:
            df = pd.DataFrame(transactions)

            # Rename columns to match Snowflake table (uppercase)
            col_map = {
                'transaction_id':    'TRANSACTION_ID',
                'card_id':           'CARD_ID',
                'event_time':        'EVENT_TIME',
                'amount':            'AMOUNT',
                'merchant':          'MERCHANT',
                'merchant_category': 'MERCHANT_CATEGORY',
                'location_city':     'LOCATION_CITY',
                'location_country':  'LOCATION_COUNTRY',
                'latitude':          'LATITUDE',
                'longitude':         'LONGITUDE',
                'device':            'DEVICE',
                'is_online':         'IS_ONLINE',
                'card_present':      'CARD_PRESENT',
                'rule_anomaly':      'RULE_ANOMALY',
                'anomaly_score':     'ANOMALY_SCORE',
                'processing_time':   'PROCESSING_TIME',
            }
            df = df.rename(columns=col_map)

            # Keep only valid columns
            valid = [c for c in col_map.values() if c in df.columns]
            df = df[valid]

            success, nchunks, nrows, _ = write_pandas(
                conn=self.conn,
                df=df,
                table_name='TRANSACTIONS',
                auto_create_table=False
            )
            if success:
                logger.info(f'Snowflake: wrote {nrows} rows')
            return nrows
        except Exception as e:
            logger.error(f'Snowflake write error: {e}')
            return 0

    def query(self, sql: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df

    def close(self):
        if self.conn:
            self.conn.close()
