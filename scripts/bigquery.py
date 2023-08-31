"""this file deals with fetching and storing data from bigquery(bq) to snowflake.
Bq creates table for each day, hence below in the code max_table is referred as the latest table(latest day) we have
fetched the data for """

import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from aws_utils import AwsUtils
from common_utils import CommonUtils
import pandas as pd
import json
from snowflake_utils import SnowFlakeUtils
import itertools
import datetime


class Bigquery:
    def __init__(self):
        self.aw = AwsUtils()
        self.cu = CommonUtils(__file__)
        self.su = SnowFlakeUtils()

        self.max_col = "table_id"
        self.cred = json.loads(
            json.loads(self.aw.get_secrets("bigquery_credentials"))["bigquery_cred"]
        )
        self.bq_credentials = service_account.Credentials.from_service_account_info(
            self.cred
        )
        self.project_id = self.cred["project_id"]
        self.client = bigquery.Client(
            credentials=self.bq_credentials, project=self.project_id
        )

        self.bq_config = self.cu.get_from_global_config("bigquery")
        self.sf_config = self.cu.get_from_global_config("snowflake")

        self.data_diff = []

    def process_data(self):
        """this method is the starting point for data processing. Following actions takes place here:
        1. list all datasets
        2. loop through datasets and list all the tables from the selected dataset
        3. make a list of dates for which data needs to be fetched
        4, call process tables method with the list of dates"""

        #  list all the datasets (bigquery schemas) available in bigquery
        bq_datasets = list(self.client.list_datasets())

        for datasets in bq_datasets:
            dataset = datasets.dataset_id
            bq_tables = list(self.client.list_tables(dataset))
            logger.info(
                f"current dataset: {dataset}"
            )

            sf_destination_table_name = json.loads(self.bq_config["table_relation"])[
                dataset
            ]

            logger.info("retrieving last offset from snowflake table")
            max_table = self.cu.get_max_date(
                sf_destination_table_name, self.max_col, schema=self.bq_config["schema"]
            )
            max_table = max_table[0][0]

            if max_table is None:
                logger.info(" max date from table is null, running full-fetch")
                max_table = self.bq_config["full_fetch"]

            # as there have been instances where new rows gets inserted in the old tables in bq. So, going 10 days back
            # from max table available in snowflake to fetch data from

            logger.info(
                f"current max_date: {max_table}, going 10 days back to ensure data completeness"
            )

            date_delta = datetime.datetime.strftime(
                datetime.datetime.strptime(max_table.split("_")[1], "%Y%m%d")
                - datetime.timedelta(days=10),
                "%Y%m%d",
            )

            max_table = "events_" + date_delta
            logger.info("table offset to start data fetch from: " + max_table)

            tables_to_ingest = [
                tables.table_id
                for tables in bq_tables
                if int(tables.table_id.split("_")[1]) >= int(max_table.split("_")[1])
            ]
            logger.info("list of tables to ingest: " + str(", ".join(tables_to_ingest)))

            self.process_tables(tables_to_ingest, dataset, sf_destination_table_name)

    def process_tables(self, bq_tables, dataset, db_table_name):
        """this method does the following
        1. takes the list of dates for which data needs to be fetched
        2. process the data for selected day
        3. store the data in snowflake for each day"""

        # query to get record counts from snowflake
        db_count_query_raw = self.bq_config["db_count_query"]

        # query to get record counts from bigquery
        bq_count_query_raw = self.bq_config["db_count_query"]

        # query to fetch data from bq
        data_query_raw = self.bq_config["data_query"]

        # query to list columns from snowflake destination table
        db_col_query = self.sf_config["show_columns"]
        col_query = db_col_query.format(
            self.sf_config["db"], self.bq_config["schema"], db_table_name
        )
        col_res = self.su.execute_query(
            col_query, self.sf_config["db"], self.bq_config["schema"]
        )
        db_col_list = [col[2] for col in col_res]

        # removing column insert time as it has been set to insert current time stamp in ddl itself, having this column
        # in here will overwrite the value, or we will have to manually add current timestamp
        db_col_list.remove("INSERT_TIME")

        # looping through table to fetch and store data
        for table_name in bq_tables:
            data_list = []
            logger.info("current table " + table_name)

            # query to count total no. of records at the source
            bq_count_query = f"select count(*) from {dataset}.{table_name}"
            logger.info("executing bq count query: " + bq_count_query)
            count_res = self.client.query(bq_count_query)
            data_count_in_bq = [res[0] for res in count_res][0]

            db_count_query = db_count_query_raw.format(
                self.sf_config["db"],
                self.bq_config["schema"],
                db_table_name,
                table_name,
            )
            logger.info("executing db count query: " + db_count_query)
            db_count_query_res = self.su.execute_query(
                db_count_query, self.sf_config["db"], self.bq_config["schema"]
            )

            # Since we went 10 days back from the max offset available in snowflake. We are processing only those tables
            # for which there is a difference of records between source and destination
            if data_count_in_bq != db_count_query_res[0][0]:
                logger.info("current records at source " + str(data_count_in_bq))
                logger.info(
                    "current records at destination " + str(db_count_query_res[0][0])
                )

                data_query = data_query_raw.format(dataset, table_name)

                logger.info("executing bq data query: " + data_query)
                query_res = self.client.query(data_query)
                for row in query_res:
                    row_data = [data for data in row]
                    data_dic = {}
                    for col, data in itertools.zip_longest(db_col_list, row_data):
                        data_dic[col] = data
                    data_dic["TABLE_ID"] = table_name
                    data_list.append(data_dic)

                logger.info("creating pandas dataframe")
                df = pd.DataFrame(data_list)

                # deleting data for the old table ids, which are having fewer records than source.
                # note: we are not sure what update logic is in place at bq, so we are deleting the entire old data from
                # snowflake and fetching fresh data from bq
                delete_from_db_query = self.bq_config["delete_query"]
                delete_from_db_query = delete_from_db_query.format(
                    self.sf_config["db"],
                    self.bq_config["schema"],
                    db_table_name,
                    table_name,
                )

                # deleting records from table on if we have any.
                if db_count_query_res[0][0] > 0:
                    logger.info("deleting old data from snowflake")
                    logger.info("delete query: " + delete_from_db_query)

                    del_recs = self.su.execute_query(
                        delete_from_db_query,
                        self.sf_config["db"],
                        self.bq_config["schema"],
                    )
                    logger.info(
                        str(del_recs[0][0])
                        + " records deleted for table id "
                        + table_name
                    )

                logger.info("storing data in datatable  " + db_table_name)
                store_data = self.su.store_dataframe(
                    df, db_table_name, self.sf_config["db"], self.bq_config["schema"]
                )
                logger.info("total rows inserted " + str(store_data))
                if len(data_list) == store_data:
                    logger.info(
                        "number of rows in source and destination are same DQ for data completeness passed"
                    )
                else:
                    logger.info(
                        "there is a difference in data count between source and destination, adding table name"
                        " in a list. An exception will be raised at the end of the execution. check bottom of "
                        "logfile for the list of tables"
                    )
                    self.data_diff.append(table_name)
            else:
                logger.info(
                    "no difference in source and destination records, moving to next table"
                )


if __name__ == "__main__":
    bq = Bigquery()
    logger = bq.cu.logger
    logger.info("starting execution")

    try:
        bq.process_data()
        if len(bq.data_diff) > 1:
            table_names = ", ".join(bq.data_diff)
            logger.error("incomplete data in tables")
            logger.info("tables with incomplete data: " + table_names)
            sys.exit(1)
    except Exception as e:
        logger.exception("program failed")
        # raise Exception()
        sys.exit(1)
    finally:
        logger.info("process completed")
        logger.info("uploading log file, " + bq.cu.log_file_name)
        bq.aw.upload_file("bigquery", logger)
