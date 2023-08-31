"""this file is the single source for snowflake related activities for every API. This restricts each API to create
their own snowflake connection instance."""

from snowflake.connector.pandas_tools import write_pandas
from aws_utils import AwsUtils
import snowflake.connector
import logging
import json


class SnowFlakeUtils:
    def __init__(self):
        self.awsu = AwsUtils()
        self.logger = logging.getLogger("mylogger")

    def get_connection(self, db, schema=None):
        snow_cred = json.loads(self.awsu.get_secrets("snowflake"))

        sconn = snowflake.connector.connect(
            user=snow_cred["SNOWFLAKE_KRAKEN_USER"],
            account=snow_cred["SNOWFLAKE_ACCOUNT_ID"],
            password=snow_cred["SNOWFLAKE_KRAKEN_PASSWORD"],
            database=db,
            schema=schema,
            role=snow_cred["SNOWFLAKE_KRAKEN_ROLE"],
            warehouse="DATA_LOADER",
        )

        return sconn

    def store_dataframe(self, df, table_name, db, schema):
        conn = self.get_connection(db, schema)
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
        conn.close()
        return nrows

    def execute_query(self, query, db, schema):
        conn = self.get_connection(db, schema)
        snowcur = conn.cursor()
        try:
            snowcur.execute(query)
            res = snowcur.fetchall()
            snowcur.close()
            conn.close()
            return res
        except Exception as e:
            snowcur.close()
            conn.close()
            raise Exception


if __name__ == "__main__":
    su = SnowFlakeUtils()
    # query = "select count(*) from raw.bigquery.bigquery_events"
    # query = "CREATE schema if not exists raw.typeform"
    query = "drop table raw.deputy.operationalunit"

    res = su.execute_query(query, "raw", "")
    print(res)
