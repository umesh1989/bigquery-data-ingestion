"""this file contains the common methods that are supposed to be shared by all the data APIs."""
import sys
from configparser import SectionProxy
from snowflake_utils import SnowFlakeUtils
import configparser
import os
import datetime
import logging


class CommonUtils:
    def __init__(self, called_by):
        self.swu = SnowFlakeUtils()
        self.module_name = called_by.split("/")[-1][:-3]

        self.ddl_path = "../ddls/"
        self.curr_path = os.path.dirname(__file__)
        self.out_path = os.path.realpath(os.path.join(self.curr_path, "..", "output"))
        self.config_path = os.path.realpath(
            os.path.join(self.curr_path, "..", "global_config.ini")
        )

        self.config_parser = configparser.ConfigParser()
        self.config_parser.read(self.config_path)
        self.sf_config = self.get_from_global_config("snowflake")

        self.log_folder = "../logs/" + self.module_name + "/"
        self.log_file_name = (
            self.module_name
            + "_exec_logs_"
            + datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            + ".log"
        )
        self.log_full_path = self.log_folder + self.log_file_name
        self.logger = self.create_logger()

    def create_logger(self) -> logging.Logger:
        """
        Logger writes to a file and also prints to stdout
        :return: Logger instance
        """
        # create all intermediate-level directories for logging
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)

        logger = logging.getLogger("data_ingestion")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s ==> %(message)s")

        file_handler = logging.FileHandler(self.log_full_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def get_from_global_config(self, section_name: str) -> SectionProxy:
        """
        Get respective config data from global config file
        :param section_name: section in global_config.ini
        :return:
        """
        return self.config_parser[section_name]

    def get_max_date(
        self, table_name, column, where="", schema=""
    ):  # this method will be invoked by each API to get max date for each API
        snowflake_static_data = self.config_parser["snowflake"]
        db = snowflake_static_data["db"]
        schema = schema
        max_query = snowflake_static_data["max_date_query"]
        query = max_query.format(column, f"{db}.{schema}.{table_name}") + where
        self.logger.info("executing query " + query)
        query_res = self.swu.execute_query(query, db, schema)

        return query_res

    def check_and_drop_duplicates(self, table_name, schema):

        self.logger.info(f"removing duplicates from table {table_name} if any")

        self.logger.info("checking for duplicates: ")
        dup_check = self.sf_config["check_duplicates"].format(table_name)

        self.logger.info("duplicate check query: " + dup_check)
        dup_recs = self.swu.execute_query(dup_check, self.sf_config["db"], schema)
        dup_ids = [i[0] for i in dup_recs]
        self.logger.info("duplicate ids: " + str(dup_ids))

        dup_query = self.sf_config["drop_duplicates"].format(
            table_name, table_name, table_name
        )
        self.logger.info("drop duplicate query: " + dup_query)
        del_recs = self.swu.execute_query(
            dup_query,
            self.sf_config["db"],
            schema,
        )
        self.logger.info(str(del_recs[0][0]) + " records deleted")


if __name__ == "__main__":
    cu = CommonUtils(__file__)
