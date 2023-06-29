"""Snowflake Dataconn Connection Implementation"""
import glob
import sys
import os
from os import environ
from cml.data_v1.customconnection import CustomConnection


os.environ["SQLALCHEMY_SILENCE_UBER_WARNING"]="1"

class SnowflakeCustomImp(CustomConnection):
    """Using SQL Alchemy toolkit to connect to Snowflake DBs"""

    """ Print information on requirements and how to use the implemented functions in this module """
    def print_usage(self):
        print(
            """
Generic Snowflake CML Python Dataconnection
Prerequisite packages:
!pip install --upgrade snowflake-sqlalchemy
pip install pandas
Connection Parameters:
(These can be set in the Dataconnection defintion created by Admins or set as User Envs for the CML Session as fallback)
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USERNAME
  SNOWFLAKE_PWD
Functions:
  print_usage()
  get_pandas_dataframe(query) -- Get a pandas dataframe for a specified sql query
--Sample Usage--
import cml.data_v1 as cmldata
CONNECTION_NAME = "%s"
conn = cmldata.get_connection(CONNECTION_NAME)
cursor = conn.get_cursor()
"""
            % (self.app_name)
        )

    """ Set up a connection to the Snowflake DB """
    def get_base_connection(self):
        try:
            from sqlalchemy import create_engine
            from snowflake.sqlalchemy import URL
        except ImportError:
            raise ImportError(
                'sqlalchemy module for snowflake not found, install it with "pip install --upgrade snowflake-sqlalchemy"'
            )
            raise
            
        engine = create_engine(URL(
            user= self.sf_user,
            password= self.sf_pwd,
            account= self.sf_account,
            database = self.sf_db ,
            schema = self.sf_schema,
            warehouse = self.sf_warehouse,
            role= self.sf_role,
        ))
        
        # setting some private variables
        conn = engine.connect()
        self._engine = engine
        self._conn = conn
            
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
        dataFrame = pds.read_sql_query(query, self._engine)
        pds.set_option("display.expand_frame_repr", False)
        return dataFrame



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
        self.sf_account = self.check_params_or_env("SNOWFLAKE_ACCOUNT")
        self.sf_user = self.check_params_or_env("SNOWFLAKE_USERNAME")
        self.sf_pwd = self.check_params_or_env("SNOWFLAKE_PWD")
        self.sf_warehouse = self.check_params_or_env("SNOWFLAKE_WAREHOUSE") 
        self.sf_schema = self.check_params_or_env("SNOWFLAKE_SCHEMA")         
        self.sf_db = self.check_params_or_env("SNOWFLAKE_DEFAULTDB") 
        self.sf_role=self.check_params_or_env("SNOWFLAKE_ROLE")