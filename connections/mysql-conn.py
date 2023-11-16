import glob
import sys
import os
from cml.data_v1.customconnection import CustomConnection

class MySQLCustomImp(CustomConnection):
    """Using mysql-connector-python to connect to MySQL DBs"""

    """ Print information on requirements and how to use the implemented functions in this module """
    def print_usage(self):
        print(
            """
            Sample MYSQL CML Python Dataconnection
            Prerequisite packages:
            pip install mysql-connector-python
            pip install pandas
            Connection Parameters:
            (These can be set in the Dataconnection defintion created by Admins or set as User Envs for the CML Session as fallback)
              MYSQL_HOST
              MYSQL_PORT
              MYSQL_USER
              MYSQL_DB
              (A password is not required to connect to the example database)

            Functions:
              get_pandas_dataframe(query) -- Get a pandas dataframe for a specified sql query
            --Sample Usage--
            import cml.data_v1 as cmldata
            CONNECTION_NAME = "%s"
            conn = cmldata.get_connection(CONNECTION_NAME)
            results = pd.get
            """
            % (self.app_name)
        )

    def get_connection(self):
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                'mysql.connector module not found, install it with "pip install mysql-connector-python"'
            )
        connection = mysql.connector.connect(host=self.mysqlhost,
                                         database=self.mysqldb,
                                         user=self.mysqluser,
                                         port=self.mysqlport)
        return connection

    """Get a pandas dataframe for the mysql connection"""
    def get_pandas_dataframe(self, SQLQUERY):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'pandas module not found, install it with "pip install pandas"'
            )
        connection = self.get_connection()
        df = pd.read_sql(SQLQUERY, connection)
        pd.set_option("display.expand_frame_repr", False)
        return df

    """ Helper function for override_parameters that gets parameters from the loaded self.parameters or ENV vars """
    def check_params_or_env(self, name):
        if self.parameters.get(name) is not None:
            return self.parameters.get(name)
        else:
            if os.environ.get(name) is not None:
                return os.environ.get(name)
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
        self.mysqlhost = self.check_params_or_env("MYSQL_HOST")
        self.mysqlport = self.check_params_or_env("MYSQL_PORT")
        self.mysqldb = self.check_params_or_env("MYSQL_DB")
        self.mysqluser = self.check_params_or_env("MYSQL_USER")
