"""Postgres Dataconn Connection Implementation"""
import glob
import sys
from os import environ
from cml.data_v1.customconnection import CustomConnection


class PostgresCustomImp(CustomConnection):
    """Using https://pypi.python.org/pypi/postgres to connect to Postgres DBs"""

    """ Print information on requirements and how to use the implemented functions in this module """
    def print_usage(self):
        print(
            """
Generic Postgres CML Python Dataconnection
Prerequisite packages:
pip install postgres
pip install pandas
Connection Parameters:
(These can be set in the Dataconnection defintion created by Admins or set as User Envs for the CML Session as fallback)
  PG_HOST
  PG_PORT
  PG_USER
  PG_DB

(This should be set by the user in their session)
  PG_PASS
Functions:
  get_cursor() -- Get a basic psycopg2 cursor
  get_pandas_dataframe(query) -- Get a pandas dataframe for a specified sql query
--Sample Usage--
import cml.data_v1 as cmldata
CONNECTION_NAME = "%s"
conn = cmldata.get_connection(CONNECTION_NAME)
cursor = conn.get_cursor()
"""
            % (self.app_name)
        )

    """ Set up a connection to the postgres server """
    def get_base_connection(self):
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                'psycopg2 module not found, install it with "pip install postgres"'
            )
        conn_string = (
            "host=" + self.pg_host
            + " port=" + self.pg_port
            + " dbname=" + self.pg_db
            + " user=" + self.pg_user
            + " password=" + self.pg_pass
        )
        conn = psycopg2.connect(conn_string)
        return conn

    """ Get a pandas dataframe for the postgres connection """
    def get_pandas_dataframe(self, query):
        try:
            import pandas as pds
        except ImportError:
            raise ImportError(
                'pandas module not found, install it with "pip install pandas"'
            )
        conn = self.get_base_connection()
        dataFrame = pds.read_sql(query, conn)
        pds.set_option("display.expand_frame_repr", False)
        return dataFrame


    """ Get a pysopg2 cursor for the postgres connection """
    def get_cursor(self):
        conn = self.get_base_connection()
        try:
            import psycopg2.extras
        except ImportError:
            raise ImportError(
                'psycopg2 module not found, install it with "pip install postgres"'
            )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cursor

    """ Helper function for override_parameters that gets parameters from the loaded self.parameters or ENV vars """
    def check_params_or_env(self, name):
        if self.parameters.get(name) is not None:
            return self.parameters.get(name)
        else:
            if environ.get(name) is not None:
                return environ.get(name)
            else:
                sys.exit(
                    "No %s specified in CML Dataconn params or ENV fallback" % name
                )

    """ Set up connection parameters that will be needed by other functions """
    def override_parameters(self):
        print(
            "Checking Connection parameters from CML DataConnections service, fall back to ENV vars if needed"
        )
        print(self.parameters)
        self.pg_host = self.check_params_or_env("PG_HOST")
        self.pg_port = self.check_params_or_env("PG_PORT")
        self.pg_db = self.check_params_or_env("PG_DB")
        self.pg_user = self.check_params_or_env("PG_USER")

        self.pg_pass = os.getenv("PG_PASS")
