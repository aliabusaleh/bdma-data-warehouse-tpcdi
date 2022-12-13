import os
import glob
import numpy as np
import pandas as pd
from datetime import datetime, date
import xmltodict
import warnings
warnings.filterwarnings('ignore')

from utils import prepare_char_insertion, prepare_numeric_insertion, get_mysql_conn, get_cust_phone, to_upper, get_mysql_engine, get_prospect



class TPCDI_Loader():
    BASE_MYSQL_CMD = ""

    def __init__(self, sf, db_name, config, batch_number, overwrite=False):
        """
    Initialize staging database.

    Attributes:
        sf (str): Scale factor to be used in benchmark.
        db_name (str): Name of database schema to which the data will be loaded.    
        config (config list): Config object retrieved from calling ConfigParser().read().
        batch_number (int): Batch number that going to be processed
    """

        self.sf = sf
        self.db_name = db_name
        self.batch_number = batch_number
        self.config = config
        self.batch_dir = "staging/" + self.sf + "/Batch" + str(self.batch_number) + "/"

        # Construct base mysql command (set host, port, and user)
        TPCDI_Loader.BASE_MYSQL_CMD = "mysql -h " + config['MEMSQL_SERVER']['memsql_host'] + " -u" + \
                                      config['MEMSQL_SERVER']['memsql_user'] + " -p" + \
                                      config['MEMSQL_SERVER']['memsql_pswd'] + " -P" + \
                                      config['MEMSQL_SERVER']['memsql_port']

        # Drop database if it is exist and overwrite param is set to True
        if overwrite:
            drop_db_query = "DROP DATABASE IF EXISTS " + self.db_name
            drop_db_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -e '" + drop_db_query + "'"
            os.system(drop_db_cmd)

        db_creation_ddl = "CREATE DATABASE " + self.db_name

        # Construct MySQL client bash command and then execute it
        db_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -e '" + db_creation_ddl + "'"
        os.system(db_creation_cmd)

        # Insert create batch date table
        batch_date_ddl = "CREATE TABLE batch_date(batch_number NUMERIC(3), batch_date DATE);"
        batch_date_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + batch_date_ddl + "\""
        os.system(batch_date_creation_cmd)

        # Enable infile load
        batch_date_ddl = "SET GLOBAL local_infile=1;"
        batch_date_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -e \"" + batch_date_ddl + "\""
        os.system(batch_date_creation_cmd)

        # Enable infile load
        sql_model_ddl = "SET GLOBAL sql_mode = 'NO_ENGINE_SUBSTITUTION';"
        batch_date_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -e \"" + sql_model_ddl + "\""
        os.system(batch_date_creation_cmd)

    def load_current_batch_date(self):
        with open(self.batch_dir + "BatchDate.txt", "r") as batch_date_file:
            batch_date_loading_query = "INSERT INTO batch_date VALUES (%i, STR_TO_DATE('%s','%s'));" % (
                self.batch_number, batch_date_file.read().strip(), "%Y-%m-%d")
            batch_date_loading_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + batch_date_loading_query + "\""
            os.system(batch_date_loading_cmd)

    def load_dim_date(self):
        """
    Create DimDate table in the staging database and then load rows in Date.txt into it.
    """

        # Create ddl to store dimDate
        dimDate_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DimDate (
      SK_DateID INTEGER NOT NULL PRIMARY KEY,
			DateValue DATE NOT NULL,
			DateDesc CHAR(20) NOT NULL,
			CalendarYearID NUMERIC(4) NOT NULL,
			CalendarYearDesc CHAR(20) NOT NULL,
			CalendarQtrID NUMERIC(5) NOT NULL,
			CalendarQtrDesc CHAR(20) NOT NULL,
			CalendarMonthID NUMERIC(6) NOT NULL,
			CalendarMonthDesc CHAR(20) NOT NULL,
			CalendarWeekID NUMERIC(6) NOT NULL,
			CalendarWeekDesc CHAR(20) NOT NULL,
			DayOfWeeknumeric NUMERIC(1) NOT NULL,
			DayOfWeekDesc CHAR(10) NOT NULL,
			FiscalYearID NUMERIC(4) NOT NULL,
			FiscalYearDesc CHAR(20) NOT NULL,
			FiscalQtrID NUMERIC(5) NOT NULL,
			FiscalQtrDesc CHAR(20) NOT NULL,
			HolidayFlag BOOLEAN
    );
    """

        # Create query to load text data into dimDate table
        dimDate_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/Date.txt' INTO TABLE DimDate COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        dimDate_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dimDate_ddl + "\""
        dimDate_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + dimDate_load_query + "\""

        # Execute the command
        os.system(dimDate_ddl_cmd)
        os.system(dimDate_load_cmd)

    def init_di_messages(self):
        """
    Create DImessages table in the target database.
    """

        # Create ddl to store dimTime
        diMessages_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DImessages (
      MessageDateAndTime TIMESTAMP NOT NULL,
			BatchID NUMERIC(5) NOT NULL,
			MessageSource CHAR(30),
			MessageText CHAR(50) NOT NULL,
			MessageType CHAR(12) NOT NULL,
			MessageData CHAR(100)
    );
    """

        diMessages_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + diMessages_ddl + "\""
        os.system(diMessages_ddl_cmd)

    def load_dim_time(self):
        """
    Create DimTime table in the staging database and then load rows in Time.txt into it.
    """

        # Create ddl to store dimTime
        dimTime_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DimTime (
      SK_TimeID INTEGER Not NULL PRIMARY KEY,
			TimeValue TIME Not NULL,
			HourID numeric(2) Not NULL,
			HourDesc CHAR(20) Not NULL,
			MinuteID numeric(2) Not NULL,
			MinuteDesc CHAR(20) Not NULL,
			SecondID numeric(2) Not NULL,
			SecondDesc CHAR(20) Not NULL,
			MarketHoursFlag BOOLEAN,
			OfficeHoursFlag BOOLEAN
    );
    """

        # Create query to load text data into dimTime table
        dimTime_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/Time.txt' INTO TABLE DimTime COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        dimTime_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dimTime_ddl + "\""
        dimTime_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + dimTime_load_query + "\""

        # Execute the command
        os.system(dimTime_ddl_cmd)
        os.system(dimTime_load_cmd)

    def load_industry(self):
        """
    Create Industry table in the staging database and then load rows in Industry.txt into it.
    """

        # Create ddl to store industry
        industry_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE Industry (
      IN_ID CHAR(2) Not NULL,
			IN_NAME CHAR(50) Not NULL,
			IN_SC_ID CHAR(4) Not NULL
    );
    """

        # Create query to load text data into industry table
        industry_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/Industry.txt' INTO TABLE Industry COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        industry_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + industry_ddl + "\""
        industry_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + industry_load_query + "\""

        # Execute the command
        os.system(industry_ddl_cmd)
        os.system(industry_load_cmd)

    def load_status_type(self):
        """
    Create StatusType table in the staging database and then load rows in StatusType.txt into it.
    """

        # Create ddl to store statusType
        statusType_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE StatusType (
      ST_ID CHAR(4) NOT NULL,
      ST_NAME CHAR(10) NOT NULL
    );
    """

        # Create query to load text data into statusType table
        statusType_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/StatusType.txt' INTO TABLE StatusType COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        statusType_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + statusType_ddl + "\""
        statusType_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + statusType_load_query + "\""

        # Execute the command
        os.system(statusType_ddl_cmd)
        os.system(statusType_load_cmd)

    def load_tax_rate(self):
        """
    Create TaxRate table in the staging database and then load rows in TaxRate.txt into it.
    """

        # Create ddl to store taxRate
        taxRate_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE TaxRate (
      TX_ID CHAR(4) NOT NULL,
      TX_NAME CHAR(50) NOT NULL,
			TX_RATE NUMERIC(6,5) NOT NULL
    );
    """

        # Create query to load text data into taxRate table
        taxRate_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/TaxRate.txt' INTO TABLE TaxRate COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        taxRate_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + taxRate_ddl + "\""
        taxRate_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + taxRate_load_query + "\""

        # Execute the command
        os.system(taxRate_ddl_cmd)
        os.system(taxRate_load_cmd)

    def load_trade_type(self):
        """
    Create TradeType table in the staging database and then load rows in TradeType.txt into it.
    """

        # Create ddl to store tradeType
        tradeType_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE TradeType (
      TT_ID CHAR(3) NOT NULL,
      TT_NAME CHAR(12) NOT NULL,
			TT_IS_SELL NUMERIC(1) NOT NULL,
			TT_IS_MRKT NUMERIC(1) NOT NULL
    );
    """

        # Create query to load text data into tradeType table
        tradeType_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/TradeType.txt' INTO TABLE TradeType COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        tradeType_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + tradeType_ddl + "\""
        tradeType_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + tradeType_load_query + "\""

        # Execute the command
        os.system(tradeType_ddl_cmd)
        os.system(tradeType_load_cmd)

    def load_staging_customer(self):
        """
    Create S_Customer table in the staging database and then load rows in CustomerMgmt.xml into it.
    """

        # Create ddl to store customer
        customer_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Customer(
      ActionType CHAR(9) NOT NULL,
      ActionTS CHAR(20) NOT NULL,
      C_ID INTEGER NOT NULL,
      C_TAX_ID CHAR(20),
      C_GNDR CHAR(1) NOT NULL,
      C_TIER NUMERIC(1),
      C_DOB DATE,
      C_L_NAME CHAR(25),
      C_F_NAME CHAR(20),
      C_M_NAME CHAR(1),
      C_ADLINE1 CHAR(80),
      C_ADLINE2 CHAR(80),
      C_ZIPCODE CHAR(12),
      C_CITY CHAR(25),
      C_STATE_PROV CHAR(20),
      C_CTRY CHAR(24),
      C_PRIM_EMAIL CHAR(50),
      C_ALT_EMAIL CHAR(50),
      C_PHONE_1_C_CTRY_CODE CHAR(30),
      C_PHONE_1_C_AREA_CODE CHAR(30),
      C_PHONE_1_C_LOCAL CHAR(30),
      C_PHONE_1_C_EXT CHAR(30),
      C_PHONE_2_C_CTRY_CODE CHAR(30),
      C_PHONE_2_C_AREA_CODE CHAR(30),
      C_PHONE_2_C_LOCAL CHAR(30),
      C_PHONE_2_C_EXT CHAR(30),
      C_PHONE_3_C_CTRY_CODE CHAR(30),
      C_PHONE_3_C_AREA_CODE CHAR(30),
      C_PHONE_3_C_LOCAL CHAR(30),
      C_PHONE_3_C_EXT CHAR(30),
      C_LCL_TX_ID CHAR(4),
      C_NAT_TX_ID CHAR(4),
      CA_ID INTEGER NOT NULL,
      CA_TAX_ST NUMERIC(1),
      CA_B_ID INTEGER,
      CA_NAME CHAR(50)
    );
    """

        # Construct mysql client bash command to execute ddl and data loading query
        customer_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + customer_ddl + "\""

        # Execute the command
        os.system(customer_ddl_cmd)

        s_customer_base_query = "INSERT INTO S_Customer VALUES "
        s_customer_values = []
        max_packet = 150

        with open("staging/" + self.sf + "/Batch1/CustomerMgmt.xml") as fd:
            doc = xmltodict.parse(fd.read())
            actions = doc['TPCDI:Actions']['TPCDI:Action']
            for action in actions:
                ActionType = prepare_char_insertion(action['@ActionType'])
                ActionTS = prepare_char_insertion(action['@ActionTS'])
                C_ID = prepare_numeric_insertion(action['Customer']['@C_ID'])
                C_TAX_ID = C_GNDR = C_L_NAME = C_F_NAME = C_M_NAME = C_ADLINE1 = C_ADLINE2 = C_ZIPCODE = C_CITY = C_STATE_PROV = C_CTRY = "''"
                C_TIER = -1
                try:
                    C_TAX_ID = prepare_char_insertion(action['Customer']['@C_TAX_ID'])
                except:
                    pass
                try:
                    C_GNDR = prepare_char_insertion(action['Customer']['@C_GNDR'])
                except:
                    pass
                try:
                    C_TIER = prepare_numeric_insertion(action['Customer']['@C_TIER'])
                except:
                    pass
                try:
                    C_DOB = prepare_char_insertion(action['Customer']['@C_DOB'])
                except:
                    pass
                try:
                    C_L_NAME = prepare_char_insertion(action['Customer']['Name']['C_L_NAME'])
                except:
                    pass
                try:
                    C_F_NAME = prepare_char_insertion(action['Customer']['Name']['C_F_NAME'])
                except:
                    pass
                try:
                    C_M_NAME = prepare_char_insertion(action['Customer']['Name']['C_M_NAME'])
                except:
                    pass
                try:
                    C_ADLINE1 = prepare_char_insertion(action['Customer']['Address']['C_ADLINE1'])
                except:
                    pass
                try:
                    C_ADLINE2 = prepare_char_insertion(action['Customer']['Address']['C_ADLINE2'])
                except:
                    pass
                try:
                    C_ZIPCODE = prepare_char_insertion(action['Customer']['Address']['C_ADLINE2'])
                except:
                    pass
                try:
                    C_CITY = prepare_char_insertion(action['Customer']['Address']['C_CITY'])
                except:
                    pass
                try:
                    C_STATE_PROV = prepare_char_insertion(action['Customer']['Address']['C_STATE_PROV'])
                except:
                    pass
                try:
                    C_CTRY = prepare_char_insertion(action['Customer']['Address']['C_CTRY'])
                except:
                    pass
                C_PRIM_EMAIL = C_ALT_EMAIL = C_PHONE_1_C_CTRY_CODE = C_PHONE_1_C_AREA_CODE = C_PHONE_1_C_LOCAL = C_PHONE_2_C_CTRY_CODE = C_PHONE_2_C_AREA_CODE = C_PHONE_2_C_LOCAL = C_PHONE_3_C_CTRY_CODE = C_PHONE_3_C_AREA_CODE = C_PHONE_3_C_LOCAL = C_PHONE_1_C_EXT = C_PHONE_2_C_EXT = C_PHONE_3_C_EXT = "NULL"
                try:
                    C_PRIM_EMAIL = prepare_char_insertion(action['Customer']['ContactInfo']['C_PRIM_EMAIL'])
                except:
                    pass
                try:
                    C_ALT_EMAIL = prepare_char_insertion(action['Customer']['ContactInfo']['C_ALT_EMAIL'])
                except:
                    pass
                try:
                    C_PHONE_1_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_1']['C_EXT'])
                except:
                    pass
                try:
                    C_PHONE_1_C_LOCAL = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_1']['C_LOCAL'])
                except:
                    pass
                try:
                    C_PHONE_1_C_AREA_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_1']['C_AREA_CODE'])
                except:
                    pass
                try:
                    C_PHONE_1_C_CTRY_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_1']['C_CTRY_CODE'])
                except:
                    pass
                try:
                    C_PHONE_2_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_2']['C_EXT'])
                except:
                    pass
                try:
                    C_PHONE_2_C_LOCAL = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_2']['C_LOCAL'])
                except:
                    pass
                try:
                    C_PHONE_2_C_AREA_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_2']['C_AREA_CODE'])
                except:
                    pass
                try:
                    C_PHONE_2_C_CTRY_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_2']['C_CTRY_CODE'])
                except:
                    pass
                try:
                    C_PHONE_3_C_EXT = prepare_char_insertion(action['Customer']['ContactInfo']['C_PHONE_3']['C_EXT'])
                except:
                    pass
                try:
                    C_PHONE_3_C_LOCAL = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_3']['C_LOCAL'])
                except:
                    pass
                try:
                    C_PHONE_3_C_AREA_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_3']['C_AREA_CODE'])
                except:
                    pass
                try:
                    C_PHONE_3_C_CTRY_CODE = prepare_char_insertion(
                        action['Customer']['ContactInfo']['C_PHONE_3']['C_CTRY_CODE'])
                except:
                    pass
                C_LCL_TX_ID = C_NAT_TX_ID = "NULL"
                try:
                    C_LCL_TX_ID = prepare_char_insertion(action['Customer']['TaxInfo']['C_LCL_TX_ID'])
                    C_NAT_TX_ID = prepare_char_insertion(action['Customer']['TaxInfo']['C_NAT_TX_ID'])
                except:
                    pass
                CA_ID = 0
                try:
                    CA_ID = prepare_numeric_insertion(action['Customer']['Account']['@CA_ID'])
                except:
                    pass
                CA_TAX_ST = "NULL"
                try:
                    CA_TAX_ST = prepare_numeric_insertion(action['Customer']['Account']['@CA_TAX_ST'])
                except:
                    pass
                CA_B_ID = "NULL"
                try:
                    CA_B_ID = prepare_numeric_insertion(action['Customer']['Account']['CA_B_ID'])
                except:
                    pass
                CA_NAME = "NULL"
                try:
                    CA_NAME = prepare_char_insertion(action['Customer']['Account']['CA_NAME'])
                except:
                    pass

                s_customer_values.append(
                    f"({ActionType}, {ActionTS}, {C_ID}, {C_TAX_ID}, {C_GNDR}, {C_TIER}, {C_DOB}, {C_L_NAME}, {C_F_NAME}, {C_M_NAME}, {C_ADLINE1}, {C_ADLINE2}, {C_ZIPCODE}, {C_CITY}, {C_STATE_PROV}, {C_CTRY}, {C_PRIM_EMAIL}, {C_ALT_EMAIL}, {C_PHONE_1_C_CTRY_CODE}, {C_PHONE_1_C_AREA_CODE}, {C_PHONE_1_C_LOCAL}, {C_PHONE_1_C_EXT}, {C_PHONE_2_C_CTRY_CODE}, {C_PHONE_2_C_AREA_CODE}, {C_PHONE_2_C_LOCAL}, {C_PHONE_2_C_EXT}, {C_PHONE_3_C_CTRY_CODE}, {C_PHONE_3_C_AREA_CODE}, {C_PHONE_3_C_LOCAL}, {C_PHONE_3_C_EXT}, {C_LCL_TX_ID}, {C_NAT_TX_ID}, {CA_ID}, {CA_TAX_ST}, {CA_B_ID}, {CA_NAME})")
                if len(s_customer_values) >= max_packet:
                    # Create query to load text data into tradeType table
                    s_customer_load_query = s_customer_base_query + ','.join(s_customer_values)
                    s_customer_values = []
                    # Construct mysql client bash command to execute ddl and data loading query
                    s_customer_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + s_customer_load_query + "\""

                    # Execute the command
                    os.system(s_customer_load_cmd)

    def load_staging_broker(self):
        """
    Create S_Broker table in the staging database and then load rows in HR.csv into it.
    """

        # Create ddl to store broker
        broker_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Broker(
      EmployeeID INTEGER NOT NULL,
      ManagerID INTEGER NOT NULL,
      EmployeeFirstName CHAR(30) NOT NULL,
      EmployeeLastName CHAR(30) NOT NULL,
      EmployeeMI CHAR(1),
      EmployeeJobCode NUMERIC(3),
      EmployeeBranch CHAR(30),
      EmployeeOffice CHAR(10),
      EmployeePhone CHAR(14)
    );
    """

        # Create query to load text data into broker table
        broker_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/HR.csv' INTO TABLE S_Broker COLUMNS TERMINATED BY ',';"

        # Construct mysql client bash command to execute ddl and data loading query
        broker_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + broker_ddl + "\""
        broker_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + broker_load_query + "\""

        # Execute the command
        os.system(broker_ddl_cmd)
        os.system(broker_load_cmd)

    def load_broker(self):
        """
    Create DimBroker table in the target database and then load rows in HR.csv into it.
    """

        # Create ddl to store broker
        dim_broker_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DimBroker(
      SK_BrokerID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
      BrokerID INTEGER NOT NULL,
      ManagerID INTEGER,
      FirstName CHAR(50) NOT NULL,
      LastName CHAR(50) NOT NULL,
      MiddleInitial CHAR(1),
      Branch CHAR(50),
      Office CHAR(50),
      Phone CHAR(14),
      IsCurrent BOOLEAN NOT NULL,
      BatchID INTEGER NOT NULL,
      EffectiveDate DATE NOT NULL,
      EndDate DATE NOT NULL
    );
    """

        # Create query to load text data into broker table
        dim_broker_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dim_broker_ddl + "\""
        # Execute the command
        os.system(dim_broker_ddl_cmd)

        load_dim_broker_query = """
      INSERT INTO DimBroker (BrokerID,ManagerID,FirstName,LastName,MiddleInitial,Branch,Office,Phone,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT SB.EmployeeID, SB.ManagerID, SB.EmployeeFirstName, SB.EmployeeLastName, SB.EmployeeMI, SB.EmployeeBranch, SB.EmployeeOffice, SB.EmployeePhone, TRUE, 1, (SELECT d_d.DateValue FROM DimDate d_d ORDER BY d_d.DateValue ASC LIMIT 1), STR_TO_DATE('99991231','%Y%m%d')
      FROM S_Broker SB
      WHERE SB.EmployeeJobCode = 314;
    """
        load_dim_broker_cmd = dim_broker_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + load_dim_broker_query + "\""
        os.system(load_dim_broker_cmd)

    def load_staging_cash_balances(self):
        """
    Create S_Cash_Balances table in the staging database and then load rows in CashTransaction.txt into it.
    """

        # Create ddl to store cash balances
        cash_balances_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Cash_Balances(
      CT_CA_ID INTEGER NOT NULL,
      CT_DTS DATE NOT NULL,
      CT_AMT CHAR(20) NOT NULL,
      CT_NAME CHAR(100) NOT NULL
    );
    """

        # Create query to load text data into prospect table
        cash_balances_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/CashTransaction.txt' INTO TABLE S_Cash_Balances COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        cash_balances_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + cash_balances_ddl + "\""
        cash_balances_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + cash_balances_load_query + "\""

        # Execute the command
        os.system(cash_balances_ddl_cmd)
        os.system(cash_balances_load_cmd)

    def load_staging_watches(self):
        """
    Create S_Watches table in the staging database and then load rows in WatchHistory.txt into it.
    """

        # Create ddl to store watches
        watches_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Watches(
      W_C_ID INTEGER NOT NULL,
      W_S_SYMB CHAR(15) NOT NULL,
      W_DTS DATE NOT NULL,
      W_ACTION CHAR(4) NOT NULL
    );
    """

        # Create query to load text data into prospect table
        watches_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/WatchHistory.txt' INTO TABLE S_Watches COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        watches_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + watches_ddl + "\""
        watches_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + watches_load_query + "\""

        # Execute the command
        os.system(watches_ddl_cmd)
        os.system(watches_load_cmd)

    def load_staging_prospect(self):
        """
    Create S_Prospect table in the staging database and then load rows in Prospect.csv into it.
    """

        # Create ddl to store prospect
        prospect_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Prospect(
      AGENCY_ID CHAR(30) NOT NULL,
      LAST_NAME CHAR(30) NOT NULL,
      FIRST_NAME CHAR(30) NOT NULL,
      MIDDLE_INITIAL CHAR(1),
      GENDER CHAR(1),
      ADDRESS_LINE_1 CHAR(80),
      ADDRESS_LINE_2 CHAR(80),
      POSTAL_CODE CHAR(12),
      CITY CHAR(25) NOT NULL,
      STATE CHAR(20) NOT NULL,
      COUNTRY CHAR(24),
      PHONE CHAR(30),
      INCOME NUMERIC(9),
      NUMBER_CARS NUMERIC(2),
      NUMBER_CHILDREM NUMERIC(2),
      MARITAL_STATUS CHAR(1),
      AGE NUMERIC(3),
      CREDIT_RATING NUMERIC(4),
      OWN_OR_RENT_FLAG CHAR(1),
      EMPLOYER CHAR(30),
      NUMBER_CREDIT_CARDS NUMERIC(2),
      NET_WORTH NUMERIC(12)
    );
    """

        # Create query to load text data into prospect table
        prospect_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/Prospect.csv' INTO TABLE S_Prospect COLUMNS TERMINATED BY ',';"

        # Construct mysql client bash command to execute ddl and data loading query
        prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + prospect_ddl + "\""
        prospect_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + prospect_load_query + "\""

        # Execute the command
        os.system(prospect_ddl_cmd)
        os.system(prospect_load_cmd)

    def load_prospect(self):
        """
    Create Prospect table in the target database and then load rows in Prospect.csv into it.
    """

        # Create ddl to store prospect
        prospect_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE Prospect (
      AgencyID CHAR(30) NOT NULL,  
			SK_RecordDateID INTEGER NOT NULL,
      SK_UpdateDateID INTEGER NOT NULL,
			BatchID NUMERIC(5) NOT NULL,
			IsCustomer BOOLEAN NOT NULL,
			LastName CHAR(30) NOT NULL,
			FirstName CHAR(30) NOT NULL,
			MiddleInitial CHAR(1),
			Gender CHAR(1),
			AddressLine1 CHAR(80),
			AddressLine2 CHAR(80),
			PostalCode CHAR(12),
			City CHAR(25) NOT NULL,
			State CHAR(20) NOT NULL,
			Country CHAR(24),
			Phone CHAR(30), 
			Income NUMERIC(9),
			NumberCars NUMERIC(2), 
			NumberChildren NUMERIC(2), 
			MaritalStatus CHAR(1), 
			Age NUMERIC(3),
			CreditRating NUMERIC(4),
			OwnOrRentFlag CHAR(1), 
			Employer CHAR(30),
			NumberCreditCards NUMERIC(2), 
			NetWorth NUMERIC(12),
			MarketingNameplate CHAR(100)
    );
    """

        # Create query to load text data into prospect table
        prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + prospect_ddl + "\""
        # Execute the command
        os.system(prospect_ddl_cmd)

        # Create function to get marketing_nameplate
        marketing_nameplate_function_definition_ddl = """
    DROP FUNCTION IF EXISTS get_marketing_template;
    DELIMITER //
    CREATE FUNCTION get_marketing_template(net_worth NUMERIC, income NUMERIC,
        number_credit_cards NUMERIC, number_children NUMERIC, age NUMERIC,
        credit_rating NUMERIC, number_cars NUMERIC) RETURNS  CHAR(100) DETERMINISTIC
      BEGIN
      DECLARE marketing_template CHAR(100);
    set marketing_template = '';
        IF (net_worth>1000000 OR income>200000) THEN
          set marketing_template =  CONCAT(marketing_template, '+HighValue');
        END IF;
        IF (number_credit_cards>5 OR number_children>3) THEN
          set marketing_template =  CONCAT(marketing_template, '+Expenses');
        END IF;
        IF (age>45) THEN
          set marketing_template = CONCAT(marketing_template, '+Boomer');
        END IF;
        IF (credit_rating<600 or income <5000 or net_worth < 100000) THEN
          set marketing_template =  CONCAT(marketing_template, '+MoneyAlert');
        END IF;
        IF (number_cars>3 or number_credit_cards>7) THEN
          set marketing_template =  CONCAT(marketing_template, '+Spender');
        END IF;
        IF (age>25 or net_worth>1000000) THEN
          set marketing_template =  CONCAT(marketing_template, '+Inherited');
        END IF;
        RETURN RIGHT(marketing_template,character_length(marketing_template)-1);
      END //
    DELIMITER ;
    """
        marketing_nameplate_function_definition_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + marketing_nameplate_function_definition_ddl + "\""
        print("create function using command")
        print(marketing_nameplate_function_definition_cmd)
        os.system(marketing_nameplate_function_definition_cmd)

        load_prospect_query = """
    INSERT INTO Prospect
    SELECT SP.AGENCY_ID, (SELECT b_d.batch_date FROM batch_date b_d WHERE b_d.batch_number=%s) SK_RecordDateID,
          (SELECT b_d.batch_date FROM batch_date b_d WHERE b_d.batch_number=%s) SK_UpdateDateID, %s, FALSE, SP.LAST_NAME,
          SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
          SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
          SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, (SELECT get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE,
                      SP.CREDIT_RATING, SP.NUMBER_CARS) ) MarketingNameplate
    FROM S_Prospect SP;

    INSERT INTO DImessages
	    SELECT current_timestamp(),%s,'Prospect', 'Inserted rows', 'Status', (SELECT COUNT(*) FROM Prospect);
    """ % (str(self.batch_number), str(self.batch_number), str(self.batch_number), str(self.batch_number))
        load_prospect_cmd = prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + load_prospect_query + "\""
        os.system(load_prospect_cmd)

    def load_audit(self):
        """
    Create Audit table in the staging database and then load rows in files with "_audit.csv" ending into it.
    """

        # Create ddl to store audit
        audit_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE Audit (
      DataSet CHAR(20) NOT Null,
			BatchID NUMERIC(5),
			AT_Date DATE,
			AT_Attribute CHAR(50),
			AT_Value NUMERIC(15),
			DValue NUMERIC(15,5)
    );
    """

        audit_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + audit_ddl + "\""
        os.system(audit_ddl_cmd)

        for filepath in glob.iglob(
                "staging/" + self.sf + "/Batch1/*_audit.csv"):  # Create query to load text data into tradeType table
            audit_load_query = "LOAD DATA LOCAL INFILE '" + filepath + "' INTO TABLE Audit COLUMNS TERMINATED BY ',' IGNORE 1 LINES;"

            # Construct mysql client bash command to execute ddl and data loading query
            audit_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + audit_load_query + "\""

            # Execute the command
            os.system(audit_load_cmd)

    def load_staging_finwire(self):
        """
    Create S_Company and S_Security table in the staging database and then load rows in FINWIRE files with the type of CMP
    """

        # Create ddl to store finwire
        finwire_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE S_Company (
      PTS CHAR(15) NOT NULL,
      REC_TYPE CHAR(3) NOT NULL,
			COMPANY_NAME CHAR(60) NOT NULL,
			CIK CHAR(10) NOT NULL,
      STATUS CHAR(4) NOT NULL,
      INDUSTRY_ID CHAR(2) NOT NULL,
			SP_RATING CHAR(4) NOT NULL,
			FOUNDING_DATE CHAR(8) NOT NULL,
      ADDR_LINE_1 CHAR(80) NOT NULL,
      ADDR_LINE_2 CHAR(80) NOT NULL,
			POSTAL_CODE CHAR(12) NOT NULL,
			CITY CHAR(25) NOT NULL,
      STATE_PROVINCE CHAR(20) NOT NULL,
      COUNTRY CHAR(24) NOT NULL,
			CEO_NAME CHAR(46) NOT NULL,
			DESCRIPTION CHAR(150) NOT NULL
    );

    CREATE TABLE S_Security (
      PTS CHAR(15) NOT NULL,
      REC_TYPE CHAR(3) NOT NULL,
			SYMBOL CHAR(15) NOT NULL,
			ISSUE_TYPE CHAR(6) NOT NULL,
      STATUS CHAR(4) NOT NULL,
      NAME CHAR(70) NOT NULL,
			EX_ID CHAR(6) NOT NULL,
			SH_OUT CHAR(13) NOT NULL,
      FIRST_TRADE_DATE CHAR(8) NOT NULL,
      FIRST_TRADE_EXCHANGE CHAR(8) NOT NULL,
			DIVIDEN CHAR(12) NOT NULL,
			COMPANY_NAME_OR_CIK CHAR(60) NOT NULL
    );

    CREATE TABLE S_Financial(
      PTS CHAR(15),
      REC_TYPE CHAR(3),
      YEAR CHAR(4),
      QUARTER CHAR(1),
      QTR_START_DATE CHAR(8),
      POSTING_DATE CHAR(8),
      REVENUE CHAR(17),
      EARNINGS CHAR(17),
      EPS CHAR(12),
      DILUTED_EPS CHAR(12),
      MARGIN CHAR(12),
      INVENTORY CHAR(17),
      ASSETS CHAR(17),
      LIABILITIES CHAR(17),
      SH_OUT CHAR(13),
      DILUTED_SH_OUT CHAR(13),
      CO_NAME_OR_CIK CHAR(60)
    );
    """

        finwire_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + finwire_ddl + "\""
        os.system(finwire_ddl_cmd)

        base_path = "staging/" + self.sf + "/Batch1/"
        s_company_base_query = "INSERT INTO S_Company VALUES "
        s_security_base_query = "INSERT INTO S_Security VALUES "
        s_financial_base_query = "INSERT INTO S_Financial VALUES"
        s_company_values = []
        s_security_values = []
        s_financial_values = []
        max_packet = 150
        for fname in os.listdir(base_path):
            if ("FINWIRE" in fname and "audit" not in fname):
                with open(base_path + fname, 'r') as finwire_file:
                    for line in finwire_file:
                        pts = line[:15]  # 0
                        rec_type = line[15:18]  # 1

                        if rec_type == "CMP":
                            company_name = line[18:78]  # 2
                            cik = line[78:88]  # 3
                            status = line[88:92]  # 4
                            industry_id = line[92:94]  # 5
                            sp_rating = line[94:98]  # 6
                            founding_date = line[98:106]  # 7
                            addr_line_1 = line[106:186]  # 8
                            addr_line_2 = line[186:266]  # 9
                            postal_code = line[266:278]  # 10
                            city = line[278:303]  # 10
                            state_province = line[303:323]  # 11
                            country = line[323:347]  # 12
                            ceo_name = line[347:393]  # 13
                            description = line[393:]  # 14

                            s_company_values.append(
                                "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                    pts, rec_type, company_name, cik, status, industry_id, sp_rating, founding_date,
                                    addr_line_1, addr_line_2, postal_code, city, state_province, country, ceo_name,
                                    description))

                            if len(s_company_values) >= max_packet:
                                # Create query to load text data into tradeType table
                                s_company_load_query = s_company_base_query + ','.join(s_company_values)
                                s_company_values = []
                                # Construct mysql client bash command to execute ddl and data loading query
                                s_company_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + s_company_load_query + "\""

                                # Execute the command
                                os.system(s_company_load_cmd)
                        elif rec_type == "SEC":
                            symbol = line[18:33]
                            issue_type = line[33:39]
                            status = line[39:43]
                            name = line[43:113]
                            ex_id = line[113:119]
                            sh_out = line[119:132]
                            first_trade_date = line[132:140]
                            first_trade_exchange = line[140:148]
                            dividen = line[148:160]
                            company_name = line[160:]

                            s_security_values.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                pts, rec_type, symbol, issue_type, status, name, ex_id, sh_out, first_trade_date,
                                first_trade_exchange, dividen, company_name))

                            if len(s_security_values) >= max_packet:
                                # Create query to load text data into tradeType table
                                s_security_load_query = s_security_base_query + ','.join(s_security_values)
                                s_security_values = []
                                # Construct mysql client bash command to execute ddl and data loading query
                                s_security_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + s_security_load_query + "\""

                                # Execute the command
                                os.system(s_security_load_cmd)
                        elif rec_type == "FIN":
                            year = line[18:22]
                            quarter = line[22:23]
                            qtr_start_date = line[23:31]
                            posting_date = line[31:39]
                            revenue = line[39:56]
                            earnings = line[56:73]
                            eps = line[73:85]
                            diluted_eps = line[85:97]
                            margin = line[97:109]
                            inventory = line[109:126]
                            assets = line[126:143]
                            liabilities = line[143:160]
                            sh_out = line[160:173]
                            diluted_sh_out = line[173:186]
                            co_name_or_cik = line[186:]

                            s_financial_values.append(
                                "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                    pts, rec_type, year, quarter, qtr_start_date, posting_date, revenue, earnings, eps,
                                    diluted_eps, margin, inventory, assets, liabilities, sh_out, diluted_sh_out,
                                    co_name_or_cik))

                            if len(s_financial_values) >= max_packet:
                                # Create query to load text data into tradeType table
                                s_financial_load_query = s_financial_base_query + ','.join(s_financial_values)
                                s_financial_values = []
                                # Construct mysql client bash command to execute ddl and data loading query
                                s_financial_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + s_financial_load_query + "\""

                                # Execute the command
                                os.system(s_financial_load_cmd)

    def load_target_dim_company(self):
        """
    Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
    """

        # Create ddl to store tradeType
        dim_company_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DimCompany (
        SK_CompanyID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
        CompanyID INTEGER NOT NULL,
        Status CHAR(10) Not NULL,
        Name CHAR(60) Not NULL,
        Industry CHAR(50) Not NULL,
        SPrating CHAR(4),
        isLowGrade BOOLEAN,
        CEO CHAR(100) Not NULL,
        AddressLine1 CHAR(80),
        AddressLine2 CHAR(80),
        PostalCode CHAR(12) Not NULL,
        City CHAR(25) Not NULL,
        StateProv CHAR(20) Not NULL,
        Country CHAR(24),
        Description CHAR(150) Not NULL,
        FoundingDate DATE,
        IsCurrent BOOLEAN Not NULL,
        BatchID numeric(5) Not NULL,
        EffectiveDate DATE Not NULL,
        EndDate DATE Not NULL
      );
    """

        # Create query to load text data into dim_company table
        dim_company_load_query = """
      INSERT INTO DimCompany (CompanyID,Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT C.CIK,S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, IF(LEFT(C.SP_RATING,1)='A' OR LEFT (C.SP_RATING,3)='BBB',FALSE,TRUE),
            C.CEO_NAME, C.ADDR_LINE_1,C.ADDR_LINE_2, C.POSTAL_CODE, C.CITY, C.STATE_PROVINCE, C.COUNTRY, C.DESCRIPTION,
            STR_TO_DATE(C.FOUNDING_DATE,'%Y%m%d'),TRUE, 1, STR_TO_DATE(LEFT(C.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
      FROM S_Company C
      JOIN Industry I ON C.INDUSTRY_ID = I.IN_ID
      JOIN StatusType S ON C.STATUS = S.ST_ID;
    """

        # Handle type 2 slowly changing dimension on company
        dim_company_sdc_query = """
    CREATE TABLE sdc_dimcompany
      LIKE DimCompany;
    ALTER TABLE sdc_dimcompany
      ADD COLUMN RN NUMERIC;
    INSERT INTO sdc_dimcompany
    SELECT *, ROW_NUMBER() OVER(ORDER BY CompanyID, EffectiveDate) RN
    FROM DimCompany;

    WITH candidate AS (
    SELECT s1.SK_CompanyID,
          s2.EffectiveDate EndDate
    FROM sdc_dimcompany s1
          JOIN sdc_dimcompany s2 ON (s1.RN = (s2.RN - 1) AND s1.CompanyID = s2.CompanyID))
    UPDATE DimCompany,candidate SET DimCompany.EndDate = candidate.EndDate, DimCompany.IsCurrent=FALSE WHERE DimCompany.SK_CompanyID = candidate.SK_CompanyID;
    DROP TABLE sdc_dimcompany;
    """

        # Construct mysql client bash command to execute ddl and data loading query
        dim_company_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dim_company_ddl + "\""
        dim_company_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + dim_company_load_query + "\""
        dim_company_sdc_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + dim_company_sdc_query + "\""

        # Execute the command
        os.system(dim_company_ddl_cmd)
        os.system(dim_company_load_cmd)
        os.system(dim_company_sdc_cmd)

    def transform_s_customer(self, tax_rate):

        NULL = ""

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT * FROM S_Customer"
        s_customer = pd.read_sql(query, conn)
        conn.close()

        df_customers = pd.DataFrame(
            columns=["CustomerID", "TaxID", "Status", "LastName", "FirstName", "MiddleInitial", "Gender", "Tier", "DOB",
                     "AddressLine1", "AddressLine2", "PostalCode", "City", "StateProv", "Country", "Phone1", "Phone2",
                     "Phone3", "Email1", "Email2", "NationalTaxRateDesc", "NationalTaxRate", "LocalTaxRateDesc",
                     "LocalTaxRate", "AgencyID", "CreditRating", "NetWorth", "MarketingNameplate", "IsCurrent",
                     "BatchID",
                     "EffectiveDate", "EndDate"])

        df_messages = pd.DataFrame(columns=["BatchID", "MessageSource", "MessageText", "MessageType", "MessageData"])

        updates = {}

        for index, row in s_customer.iterrows():
            action_type = row["ActionType"]
            if action_type in ["NEW", "UPDCUST"]:
                nrow = {
                    "CustomerID": row['C_ID'],
                    "EffectiveDate": row["ActionTS"][:10],
                    "TaxID": row['C_TAX_ID'],

                    "LastName": row["C_L_NAME"],
                    "FirstName": row["C_F_NAME"],
                    "MiddleInitial": row["C_M_NAME"],

                    "Gender": row['C_GNDR'],
                    "Tier": row['C_TIER'],
                    "DOB": row['C_DOB'],

                    "AddressLine1": row["C_ADLINE1"],
                    "AddressLine2": row["C_ADLINE2"],
                    "PostalCode": row["C_ZIPCODE"],
                    "City": row["C_CITY"],
                    "StateProv": row["C_STATE_PROV"],
                    "Country": row["C_CTRY"],

                    "Phone1": get_cust_phone(1, row),
                    "Phone2": get_cust_phone(2, row),
                    "Phone3": get_cust_phone(3, row),
                    "Email1": row["C_PRIM_EMAIL"],
                    "Email2": row["C_ALT_EMAIL"],
                }

                if nrow["Gender"] is not None and nrow["Gender"] != NULL:
                    nrow["Gender"] = nrow["Gender"].upper()
                    if nrow["Gender"] not in ["F", "M"]:
                        nrow["Gender"] = "U"

                nat_tax_id = row["C_NAT_TX_ID"]
                if nat_tax_id:
                    nrow["NationalTaxRateDesc"] = tax_rate[tax_rate.TX_ID == nat_tax_id]["TX_NAME"].iloc[0]
                    nrow["NationalTaxRate"] = tax_rate[tax_rate.TX_ID == nat_tax_id]["TX_RATE"].iloc[0]

                lcl_tax_id = row["C_LCL_TX_ID"]
                if lcl_tax_id:
                    nrow["LocalTaxRateDesc"] = tax_rate[tax_rate.TX_ID == nat_tax_id]["TX_NAME"].iloc[0]
                    nrow["LocalTaxRate"] = tax_rate[tax_rate.TX_ID == nat_tax_id]["TX_RATE"].iloc[0]

                if action_type == "NEW":
                    nrow["Status"] = "ACTIVE"
                    nrow["IsCurrent"] = True
                    nrow["BatchID"] = 1
                    nrow["EndDate"] = "9999-12-31"
                    df_new = pd.DataFrame(nrow, index=[0])
                    df_new.fillna(np.nan, inplace=True)
                    df_customers = df_customers.append(nrow, ignore_index=True)

                elif action_type == "UPDCUST":
                    if nrow["CustomerID"] not in updates:
                        updates[nrow["CustomerID"]] = []
                    updates[nrow["CustomerID"]].append(nrow)

                if "Tier" in nrow and nrow["Tier"] is not None and nrow["Tier"] != NULL:
                    if nrow["Tier"] not in ["1", "2", "3"]:
                        df_messages = df_messages.append({
                            "BatchID": 1,
                            "MessageSource": "DimCustomer",
                            "MessageType": "Alert",
                            "MessageText": "Invalid customer tier",
                            "MessageData": "C_ID = " + str(nrow["CustomerID"]) + ", C_TIER = " + str(nrow["Tier"])
                        }, ignore_index=True)

                if "DOB" in nrow and nrow["DOB"] is not None and nrow["DOB"] != NULL:
                    ds = open(self.batch_dir + "BatchDate.txt").read()[:-1]
                    batch_date = datetime.strptime(ds, "%Y-%m-%d").date()
                    min_date = date(batch_date.year - 100, batch_date.month, batch_date.day)
                    dob = nrow["DOB"]

                    if dob < min_date or dob > batch_date:
                        df_messages = df_messages.append({
                            "BatchID": 1,
                            "MessageSource": "DimCustomer",
                            "MessageType": "Alert",
                            "MessageText": "DOB out of range",
                            "MessageData": "C_ID = " + str(nrow["CustomerID"]) + ", C_DOB = " + str(nrow["DOB"])
                        }, ignore_index=True)

            elif action_type == "INACT":
                customer_id = row['C_ID']
                action_ts = row["ActionTS"][:10]

                if customer_id not in updates:
                    updates[customer_id] = []
                updates[customer_id].append(
                    {"CustomerID": customer_id, "EffectiveDate": action_ts, "Status": "INACTIVE"})

        for c_id, upds in updates.items():
            for upda in upds:
                action_ts = upda["EffectiveDate"]
                old_index = df_customers[(df_customers.CustomerID == c_id) & df_customers.IsCurrent].index.values[0]
                new_row = df_customers.loc[old_index, :].copy()

                df_customers.loc[old_index, "IsCurrent"] = False
                df_customers.loc[old_index, "EndDate"] = action_ts

                for attrib, value in upda.items():
                    if value is not None:
                        if value == "":
                            new_row[attrib] = np.nan
                        else:
                            new_row[attrib] = value

                df_customers = df_customers.append(new_row, ignore_index=True)
        return df_customers, df_messages

    def load_target_dim_customer(self):
        dim_customer_ddl = """
        
        USE """ + self.db_name + """;

        CREATE TABLE DimCustomer (
            SK_CustomerID NUMERIC(11) NOT NULL PRIMARY KEY,
            CustomerID INTEGER NOT NULL,
            TaxID CHAR(20) NOT NULL,
            Status CHAR(10) NOT NULL,
            LastName CHAR(30) NOT NULL,
            FirstName CHAR(30) NOT NULL,
            MiddleInitial CHAR(1),
            Gender CHAR(1),
            Tier NUMERIC(1),
            DOB DATE NOT NULL,
            AddressLine1 CHAR(80) NOT NULL,
            AddressLine2 CHAR(80),
            PostalCode CHAR(12) NOT NULL,
            City CHAR(25) NOT NULL,
            StateProv CHAR(20) NOT NULL,
            Country CHAR(24),
            Phone1 CHAR(30),
            Phone2 CHAR(30),
            Phone3 CHAR(30),
            Email1 CHAR(50),
            Email2 CHAR(50),
            NationalTaxRateDesc CHAR(50),
            NationalTaxRate NUMERIC(6,5),
            LocalTaxRateDesc CHAR(50),
            LocalTaxRate NUMERIC(6,5),
            AgencyID CHAR(30),
            CreditRating NUMERIC(5),
            NetWorth NUMERIC(10),
            MarketingNameplate CHAR(100),
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMERIC(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );
        """
        dim_customer_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dim_customer_ddl + "\""

        # Execute the command
        os.system(dim_customer_ddl_cmd)

        NULL = ""
        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT * FROM TaxRate"
        tax_rate = pd.read_sql(query, conn)
        conn.close()

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT * FROM Prospect"
        prospect = pd.read_sql(query, conn)
        conn.close()

        prospect["key"] = prospect.apply(lambda row: ''.join([
            pd.notna(row["LastName"]) and str(row["LastName"]) or NULL,
            pd.notna(row["FirstName"]) and str(row["FirstName"]) or NULL,
            pd.notna(row["AddressLine1"]) and str(row["AddressLine1"]) or NULL,
            pd.notna(row["AddressLine2"]) and str(row["AddressLine2"]) or NULL,
            pd.notna(row["PostalCode"]) and str(row["PostalCode"]) or NULL]), axis=1)

        prospect["key"] = prospect["key"].apply(lambda x: x.upper())
        prospect = prospect.set_index("key")

        df_customers, df_messages = self.transform_s_customer(tax_rate)

        df_customers["ProspectKey"] = df_customers["LastName"].apply(to_upper) + \
                                      df_customers["FirstName"].apply(to_upper) + \
                                      df_customers["AddressLine1"].apply(to_upper) + \
                                      df_customers["AddressLine2"].apply(to_upper) + \
                                      df_customers["PostalCode"].apply(to_upper)

        df_customers["AgencyID"] = df_customers.apply(lambda x: get_prospect(x, prospect)[0], axis=1)
        df_customers["CreditRating"] = df_customers.apply(lambda x: get_prospect(x, prospect)[1], axis=1)
        df_customers["NetWorth"] = df_customers.apply(lambda x: get_prospect(x, prospect)[2], axis=1)
        df_customers["MarketingNameplate"] = df_customers.apply(lambda x: get_prospect(x, prospect)[3], axis=1)
        df_customers.drop("ProspectKey", inplace=True, axis=1)
        df_customers.replace("", np.nan, inplace=True)
        df_customers["SK_CustomerID"] = df_customers.index

        con = get_mysql_engine(self.db_name, self.config)
        df_customers.to_sql("DimCustomer", index=False, if_exists="append", con=con)
        df_messages.to_sql("DImessages", index=False, if_exists="append", con=con)

    #TODO: ADD DDL to Dim Account, Insert DImessages as well
    def load_target_dim_account(self):

        dim_account_ddl = """
            USE """ + self.db_name + """;
            CREATE TABLE DimAccount(
                SK_AccountID NUMERIC(11) NOT NULL PRIMARY KEY,
                AccountID NUMERIC(11) NOT NULL,
                SK_BrokerID NUMERIC(11) NOT NULL,
                SK_CustomerID NUMERIC(11) NOT NULL,
                Status CHAR(10) NOT NULL,
                AccountDesc CHAR(50),
                TaxStatus NUMERIC(1) NOT NULL CHECK (TaxStatus IN (0, 1, 2)),
                IsCurrent BOOLEAN NOT NULL,
                BatchID NUMERIC(5) NOT NULL,
                EffectiveDate DATE NOT NULL,
                EndDate DATE NOT NULL
                );
            """
        dim_account_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dim_account_ddl + "\""

        # Execute the command
        os.system(dim_account_ddl_cmd)

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT * FROM S_Customer"
        s_customer = pd.read_sql(query, conn)
        conn.close()
        s_customer = s_customer[['ActionType', 'ActionTS', 'C_ID', 'CA_ID', 'CA_TAX_ST', 'CA_B_ID', 'CA_NAME']]
        s_customer.set_index('CA_ID', inplace=True)

        conn = get_mysql_conn(self.db_name,self.config)
        query = "SELECT SK_BROKERID, BrokerID FROM DimBroker"
        dim_broker = pd.read_sql(query, conn)
        conn.close()
        dim_broker.set_index('SK_BROKERID', inplace=True)

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT SK_CustomerID, CustomerID, EffectiveDate, EndDate FROM DimCustomer"
        dim_customer = pd.read_sql(query, conn)
        dim_customer.SK_CustomerID = dim_customer.SK_CustomerID.astype('int64')
        conn.close()
        dim_customer.set_index('SK_CustomerID', inplace=True)

        columns = ['AccountID', 'SK_BrokerID', 'SK_CustomerID',
                   'Status', 'AccountDesc', 'TaxStatus', 'IsCurrent',
                   'BatchID', 'EffectiveDate', 'EndDate']
        dim_account = pd.DataFrame(columns=columns)
        nrow = dict()

        for index, row in s_customer.iterrows():

            action_type = row['ActionType']
            if action_type in ["NEW", "ADDACT"]:

                nrow['AccountID'] = index
                nrow['AccountDesc'] = row.CA_NAME
                nrow['TaxStatus'] = row.CA_TAX_ST
                nrow['EffectiveDate'] = row.ActionTS[:10]
                nrow['EndDate'] = "9999-12-31"
                nrow['IsCurrent'] = 1
                nrow['BatchID'] = 1
                nrow['Status'] = 'Active'

                r = dim_broker.loc[dim_broker['BrokerID'] == row.CA_B_ID]
                if r.shape[0] > 0:
                    nrow['SK_BrokerID'] = r.index[0]

                r = dim_customer[(dim_customer['CustomerID'] == row.C_ID) &
                                 (dim_customer['EffectiveDate'] <= datetime.strptime(row.ActionTS[:10],
                                                                                     "%Y-%m-%d").date()) &
                                 (dim_customer['EndDate'] > datetime.strptime(row.ActionTS[:10], "%Y-%m-%d").date())]

                if r.shape[0] > 0:
                    nrow['SK_CustomerID'] = r.index[0]

                dim_account = dim_account.append(nrow, ignore_index=True)

            elif action_type == "UPDACCT":
                new_account = dim_account[dim_account['AccountID'] == index].tail(1).copy()
                old_account = new_account.copy()
                new_account['EffectiveDate'] = row.ActionTS[:10]
                new_account['EndDate'] = "9999-12-31"

                for field in row.index:
                    if row[field] is None:
                        continue

                    if field == 'C_ID':
                        r = dim_customer[(dim_customer['CustomerID'] == row.C_ID) &
                                         (dim_customer['EffectiveDate'] <= datetime.strptime(row.ActionTS[:10],
                                                                                             "%Y-%m-%d").date()) &
                                         (dim_customer['EndDate'] > datetime.strptime(row.ActionTS[:10],
                                                                                      "%Y-%m-%d").date())]

                        if r.shape[0] > 0:
                            new_account['SK_CustomerID'] = r.index[0]

                    if field == 'CA_TAX_ST':
                        new_account['TaxStatus'] = row.CA_TAX_ST

                    if field == 'CA_B_ID':
                        r = dim_broker.loc[dim_broker['BrokerID'] == row.CA_B_ID]

                        if r.shape[0] > 0:
                            new_account['SK_BrokerID'] = r.index[0]

                    if field == 'CA_NAME':
                        new_account['AccountDesc'] = row.CA_NAME

                old_account['EndDate'] = row.ActionTS[:10]
                old_account['IsCurrent'] = 0
                dim_account.update(old_account)
                dim_account = dim_account.append(new_account, ignore_index=True)

            elif action_type == 'CLOSEACCT':
                new_accounts = dim_account[dim_account['AccountID'] == index].tail(1).copy()
                old_accounts = new_accounts.copy()
                new_accounts['EffectiveDate'] = row.ActionTS[:10]
                new_accounts['Status'] = 'INACTIVE'
                old_accounts['EndDate'] = row.ActionTS
                dim_account.update(old_accounts)
                dim_account = dim_account.append(new_accounts, ignore_index=True)

            elif action_type == 'UPDCUST':

                customer = dim_customer[(dim_customer['CustomerID'] == row.C_ID) & (
                        (dim_customer['EffectiveDate'] == datetime.strptime(row.ActionTS[:10], "%Y-%m-%d").date()) |
                        (dim_customer['EndDate'] == datetime.strptime(row.ActionTS[:10], "%Y-%m-%d").date()))]

                if customer.empty:
                    continue

                old_accounts = dim_account[dim_account['SK_CustomerID'] == customer.head(1).index[0]].copy()
                new_accounts = old_accounts.copy()
                new_accounts.loc[:, 'SK_CustomerID'] = int(customer.tail(1).index[0])
                new_accounts.loc[:, 'EffectiveDate'] = row.ActionTS[:10]
                new_accounts.loc[:, 'EndDate'] = "9999-12-31"
                old_accounts.loc[:, 'EndDate'] = row.ActionTS[:10]
                dim_account.update(old_accounts)
                dim_account = dim_account.append(new_accounts, ignore_index=True)

            elif action_type == 'INACT':

                customer = dim_customer[(dim_customer['CustomerID'] == row.C_ID) & (
                        (dim_customer['EffectiveDate'] == row.ActionTS[:10]) |
                        (dim_customer['EndDate'] == row.ActionTS[:10]))]

                if customer.empty:
                    continue

                old_accounts = dim_account[dim_account['SK_CustomerID'] == customer.head(1).index[0]].copy()
                new_accounts = old_accounts.copy()
                new_accounts.loc[:, 'SK_CustomerID'] = int(customer.tail(1).index[0])
                new_accounts.loc[:, 'Status'] = 'INACTIVE'
                new_accounts.loc[:, 'IsCurrent'] = 0
                new_accounts.loc[:, 'EffectiveDate'] = row.ActionTS[:10]
                new_accounts.loc[:, 'EndDate'] = "9999-12-31"
                old_accounts.loc[:, 'EndDate'] = row.ActionTS[:10]

                dim_account.update(old_accounts)
                dim_account = dim_account.append(new_accounts, ignore_index=True)

        dim_account.index.name = 'SK_AccountID'
        dim_account.fillna(value={'SK_BrokerID': -1}, inplace=True)

        dim_account['SK_CustomerID'] = dim_account['SK_CustomerID'].astype('int32')
        dim_account['SK_BrokerID'] = dim_account['SK_BrokerID'].astype('int32')
        dim_account.index.astype('int32', copy=False)
        dim_account['AccountID'] = dim_account['AccountID'].astype('int32')
        dim_account['BatchID'] = dim_account['BatchID'].astype('int32')
        dim_account['IsCurrent'] = dim_account['IsCurrent'].astype('int32')

        con = get_mysql_engine(self.db_name,self.config)
        dim_account.to_sql(con=con, name='DimAccount', if_exists='append')

    def load_target_dim_trade(self):

        columns = ['T_ID', 'T_DTS', 'T_ST_ID', 'T_TT_ID',
                   'T_IS_CASH', 'T_S_SYMB', 'T_QTY', 'T_BID_PRICE',
                   'T_CA_ID', 'T_EXEC_NAME', 'T_TRADE_PRICE',
                   'T_CHRG', 'T_COMM', 'T_TAX']
        trade = pd.read_csv(self.batch_dir + 'Trade.txt', sep='|', names=columns, parse_dates=['T_DTS'],index_col='T_ID')

        columns = ['TH_T_ID', 'TH_DTS', 'TH_ST_ID']
        trade_history = pd.read_csv(self.batch_dir + 'TradeHistory.txt', sep='|', names=columns, parse_dates=['TH_DTS'],index_col='TH_T_ID')

        status_type = pd.read_csv(self.batch_dir + 'StatusType.txt', sep='|', names=['ST_ID', 'ST_NAME'],
                                  index_col='ST_ID')
        trade_type = pd.read_csv(self.batch_dir + 'TradeType.txt', sep='|',
                                 names=['TT_ID', 'TT_NAME', 'TT_IS_SELL', 'TT_IS_MRKT'], index_col='TT_ID')

        joined = trade.join(trade_history, on="T_ID")

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT SK_SecurityID, SK_CompanyID, Symbol, EffectiveDate, EndDate FROM DimSecurity"
        dim_security = pd.read_sql(query, conn)
        conn.close()
        dim_security.set_index('SK_SecurityID', inplace=True)

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT SK_AccountID, SK_CustomerID, SK_BrokerID, AccountID, EffectiveDate, EndDate FROM DimAccount"
        dim_account = pd.read_sql(query, conn)
        conn.close()
        dim_account.set_index('SK_AccountID', inplace=True)

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT SK_DateID, DateValue FROM DimDate"
        dim_date = pd.read_sql(query, conn)
        conn.close()
        dim_date.set_index('SK_DateID', inplace=True)

        conn = get_mysql_conn(self.db_name, self.config)
        query = "SELECT SK_TimeID, TimeValue FROM DimTime"
        dim_time = pd.read_sql(query, conn)
        dim_time.TimeValue = pd.to_datetime(datetime.now().date()) + dim_time.TimeValue
        conn.close()
        dim_time.set_index('SK_TimeID', inplace=True)

        columns = ['TradeID', 'SK_BrokerID', 'SK_CreateDateID', 'SK_CreateTimeID', 'SK_CloseDateID',
                   'SK_CloseTimeID', 'Status', 'Type', 'CashFlag', 'SK_SecurityID',
                   'SK_CompanyID', 'Quantity', 'BidPrice',
                   'SK_CustomerID', 'SK_AccountID', 'ExecutedBy', 'TradePrice',
                   'Fee', 'Commission', 'Tax', 'BatchID']

        dim_trade = pd.DataFrame(columns=columns)
        index_set = set()

        for index, row in joined.iterrows():

            nrow = {
                "TradeID": index,
                "CashFlag": row.T_IS_CASH,
                "Quantity": row.T_QTY,
                "BidPrice": row.T_BID_PRICE,
                "ExecutedBy": row.T_EXEC_NAME,
                "TradePrice": row.T_TRADE_PRICE,
                "Fee": row.T_CHRG,
                "Commission": row.T_COMM,
                "Tax": row.T_TAX,
                "Status": status_type.loc[row.TH_ST_ID].ST_NAME,
                "Type": trade_type.loc[row.T_TT_ID].TT_NAME,
                "BatchId": 1
            }

            if (row.TH_ST_ID == "SBMT" and row.T_TT_ID in ["TMB", "TMS"]) or row.TH_ST_ID == "PNDG":
                nrow['SK_CreateDateID'] = dim_date[dim_date['DateValue'] == row.TH_DTS.date()].index[0]
                nrow['SK_CreateTimeID'] = \
                dim_time[dim_time['TimeValue'] == row.TH_DTS.time().strftime("%H:%M:%S")].TimeValue.dt.time.index[0]
                if index not in index_set:
                    nrow['SK_CloseDateID'] = None
                    nrow['SK_CloseTimeID'] = None

            if row.TH_ST_ID in ["CMPT", "CNCL"]:
                nrow['SK_CloseDateID'] = dim_date[dim_date['DateValue'] == row.TH_DTS.date()].index[0]
                nrow['SK_CloseTimeID'] = \
                dim_time[dim_time['TimeValue'] == row.TH_DTS.time().strftime("%H:%M:%S")].TimeValue.dt.time.index[0]
                if index not in index_set:
                    nrow['SK_CreateDateID'] = None
                    nrow['SK_CreateTimeID'] = None

            if index not in index_set:
                record = dim_security[(dim_security['Symbol'] == row.T_S_SYMB) &
                                      (dim_security['EffectiveDate'] <= row.TH_DTS.date()) &
                                      (dim_security['EndDate'] > row.TH_DTS.date())]

                if record.shape[0] > 0:
                    nrow['SK_SecurityID'] = record.index[0]
                    nrow['SK_CompanyID'] = record.iloc[0]['SK_CompanyID']

                record = dim_account[(dim_account['AccountID'] == row.T_CA_ID) &
                                     (dim_account['EffectiveDate'] <= row.TH_DTS.date()) &
                                     (dim_account['EndDate'] > row.TH_DTS)]

                if record.shape[0] > 0:
                    nrow['SK_AccountID'] = record.index[0]
                    nrow['SK_CustomerID'] = record.iloc[0]['SK_CustomerID']
                    nrow['SK_BrokerID'] = record.iloc[0]['SK_BrokerID']

                index_set.add(index)
                dim_trade = dim_trade.append(nrow, ignore_index=True)
            else:
                new_row = pd.DataFrame(nrow, index=[index])
                dim_trade.update(new_row)

        dim_trade.set_index('TradeID', inplace=True)
        con = get_mysql_engine(self.db_name,self.config)
        dim_trade.to_sql(con=con, name='DimTrade', if_exists='append')

    def load_target_fact_cash_balance(self):
        """
        create FactCashBalances table
        """
        # Create ddl to store tradeType
        fact_cash_balance_ddl = """
        USE """ + self.db_name + """;

        CREATE TABLE FactCashBalances (
            SK_CustomerID INTEGER NOT NULL PRIMARY KEY,
            SK_AccountID INTEGER NOT NULL,
            SK_DateID INTEGER(10) Not NULL,
            Cash INTEGER Not NULL,
            BatchID numeric(5) Not NULL
          );
        """
        fact_cash_balance_load_query = """
              INSERT INTO FactCashBalances (SK_CustomerID,SK_AccountID,SK_DateID,Cash,BatchID)
              SELECT C.CT_CA_ID, A.SK_AccountID, D.SK_DateID , C.CT_AMT , 1
              FROM S_Cash_Balances C
              JOIN DimAccount A ON C.CT_CA_ID = A.SK_CustomerID
              JOIN DimDate D on DATE(C.CT_DTS) = D.DateValue
              where A.IsCurrent = true
              ON DUPLICATE KEY UPDATE Cash = (Cash + C.CT_AMT);
            """
        # Construct mysql client bash command to execute ddl and data loading query
        fact_cash_balance_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + fact_cash_balance_ddl + "\""
        fact_cash_balance_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + fact_cash_balance_load_query + "\""

        # Execute the command
        os.system(fact_cash_balance_ddl_cmd)
        os.system(fact_cash_balance_load_cmd)

    def load_target_dim_security(self):
        """
    Create Security table in the staging database and then load rows by ..
    """

        # Create ddl to store tradeType
        security_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE DimSecurity( SK_SecurityID INTEGER Not NULL PRIMARY KEY AUTO_INCREMENT,
      Symbol CHAR(15) Not NULL,
      Issue CHAR(6) Not NULL,
      Status CHAR(10) Not NULL,
      Name CHAR(70) Not NULL,
      ExchangeID CHAR(6) Not NULL,
      SK_CompanyID INTEGER Not NULL,
      SharesOutstanding INTEGER Not NULL,
      FirstTrade DATE Not NULL,
      FirstTradeOnExchange DATE Not NULL,
      Dividend INTEGER Not NULL,
      IsCurrent BOOLEAN Not NULL,
      BatchID numeric(5) Not NULL,
      EffectiveDate DATE Not NULL,
      EndDate DATE Not NULL
    );
    """

        # Create query to load text data into security table
        security_load_query = """
    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON DC.SK_CompanyID = convert(SS.COMPANY_NAME_OR_CIK, SIGNED)
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1)='0';

    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1) <> '0';
    """

        dim_security_scd = """
    CREATE TABLE sdc_dimsecurity
      LIKE DimSecurity;
    ALTER TABLE sdc_dimsecurity
      ADD COLUMN RN NUMERIC;
    INSERT INTO sdc_dimsecurity
    SELECT *, ROW_NUMBER() OVER(ORDER BY Symbol, EffectiveDate) RN
    FROM DimSecurity;

    WITH candidate AS (SELECT s1.SK_SecurityID, s2.EffectiveDate EndDate
                      FROM sdc_dimsecurity s1
                              JOIN sdc_dimsecurity s2 ON (s1.RN = (s2.RN - 1) AND s1.Symbol = s2.Symbol))
    UPDATE DimSecurity, candidate
    SET DimSecurity.EndDate   = candidate.EndDate,
        DimSecurity.IsCurrent = FALSE
    WHERE DimSecurity.SK_SecurityID = candidate.SK_SecurityID;
    DROP TABLE sdc_dimsecurity;
    """

        # Construct mysql client bash command to execute ddl and data loading query
        dim_security_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + security_ddl + "\""
        dim_security_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + security_load_query + "\""
        dim_security_scd_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + dim_security_scd + "\""

        # Execute the command
        os.system(dim_security_ddl_cmd)
        os.system(dim_security_load_cmd)
        os.system(dim_security_scd_cmd)

    def load_target_financial(self):
        """
    Create Financial table in the staging database and then load rows by ..
    """

        # Create ddl to store tradeType
        financial_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE Financial (
      SK_CompanyID INTEGER Not NULL,
      FI_YEAR numeric(4) Not NULL,
      FI_QTR numeric(1) Not NULL,
      FI_QTR_START_DATE DATE Not NULL,
      FI_REVENUE numeric(15,2) Not NULL,
      FI_NET_EARN numeric(15,2) Not NULL,
      FI_BASIC_EPS numeric(10,2) Not NULL,
      FI_DILUT_EPS numeric(10,2) Not NULL,
      FI_MARGIN numeric(10,2) Not NULL,
      FI_INVENTORY numeric(15,2) Not NULL,
      FI_ASSETS numeric(15,2) Not NULL,
      FI_LIABILITY numeric(15,2) Not NULL,
      FI_OUT_BASIC numeric(12) Not NULL,
      FI_OUT_DILUT numeric(12) Not NULL
      );
    """

        # Create query to load text data into financial table
        financial_load_query = """
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON DC.SK_CompanyID = convert(SF.CO_NAME_OR_CIK, SIGNED)
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1)='0';
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON RTRIM(SF.CO_NAME_OR_CIK) = DC.Name
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1) <> '0'
    """

        # Construct mysql client bash command to execute ddl and data loading query
        dim_financial_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + financial_ddl + "\""
        dim_financial_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + financial_load_query + "\""

        # Execute the command
        os.system(dim_financial_ddl_cmd)
        os.system(dim_financial_load_cmd)

    def load_staging_trade_history(self):
        """
    Create s_trade_history table in to the database and then load rows in TradeHistory.txt into it.
    """

        # Create ddl to store tade_history
        tade_history_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE s_trade_history(
      th_t_id NUMERIC(15),
      th_dts DATETIME,
      th_st_id CHAR(4)
    );

    """

        # Create query to load text data into tade_history table
        tade_history_load_query = "LOAD DATA LOCAL INFILE '" + self.batch_dir + "TradeHistory.txt' INTO TABLE s_trade_history COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        tade_history_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + tade_history_ddl + "\""
        tade_history_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + tade_history_load_query + "\""

        # Execute the command
        os.system(tade_history_ddl_cmd)
        os.system(tade_history_load_cmd)

    def load_staging_trade(self):
        """
    Create s_trade table in to the database and then load rows in Trade.txt into it.
    """

        # Create ddl to store tade
        tade_ddl = """
    USE """ + self.db_name + """;

    CREATE TABLE s_trade(
      cdc_flag CHAR(1),
      cdc_dsn NUMERIC(12),
      t_id NUMERIC(15),
      t_dts DATETIME,
      t_st_id CHAR(4),
      t_tt_id CHAR(3),
      t_is_cash CHAR(3),
      t_s_symb CHAR(15) NOT NULL,
      t_qty NUMERIC(6) NOT NULL,
      t_bid_price NUMERIC(8),
      t_ca_id NUMERIC(11),
      t_exec_name CHAR(49),
      t_trade_price NUMERIC(8),
      t_chrg NUMERIC(10),
      t_comm NUMERIC(10),
      t_tax NUMERIC(10)
    );


    """

        if self.batch_number == 1:
            # Create query to load text data into tade_ table
            tade_load_query = "LOAD DATA LOCAL INFILE '" + self.batch_dir + "Trade.txt' INTO TABLE s_trade COLUMNS TERMINATED BY '|' \
      (t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax);"
        else:
            # Create query to load text data into tade_ table
            tade_load_query = "LOAD DATA LOCAL INFILE '" + self.batch_dir + "Trade.txt' INTO TABLE s_trade COLUMNS TERMINATED BY '|'"

        # Construct mysql client bash command to execute ddl and data loading query
        tade_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + tade_ddl + "\""
        tade_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + tade_load_query + "\""

        # Execute the command
        os.system(tade_ddl_cmd)
        os.system(tade_load_cmd)

    def load_staging_fact_holding(self):
        """
    Create s_fact_holding table in the staging database and then load rows in HoldingHistory.txt into it.
    """

        # Create ddl to store watches
        holding_ddl = """
       USE """ + self.db_name + """;

       CREATE TABLE s_fact_holding(
         HH_H_T_ID INTEGER NOT NULL,
         HH_T_ID INTEGER NOT NULL,
         HH_BEFORE_QTY INTEGER NOT NULL,
         HH_AFTER_QTY INTEGER NOT NULL
       );
       """

        # Create query to load text data into prospect table
        holding_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/HoldingHistory.txt' INTO TABLE s_fact_holding COLUMNS TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        holding_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + holding_ddl + "\""
        holding_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + holding_load_query + "\""

        # Execute the command
        os.system(holding_ddl_cmd)
        os.system(holding_load_cmd)

    def load_target_fact_holding(self):
        """
        create FactHoldings table
         """
        # Create ddl to store tradeType
        fact_holding_ddl = """
               USE """ + self.db_name + """;

               CREATE TABLE FactHoldings (
                   TradeID INTEGER NOT NULL PRIMARY KEY,
                   CurrentTradeID INTEGER NOT NULL ,
                   SK_CustomerID INTEGER NOT NULL,
                   SK_AccountID INTEGER NOT NULL,
                   SK_SecurityID INTEGER NOT NULL,
                   SK_CompanyID INTEGER NOT NULL,
                   SK_DateID INTEGER Not NULL,
                   SK_TimeID INTEGER Not NULL,
                   CurrentPrice INTEGER Not NULL,
                   CurrentHolding INTEGER(6) Not NULL,
                   BatchID numeric(5) Not NULL
                 );
               """
        fact_holding_load_query = """
                     INSERT INTO FactHoldings (TradeID,CurrentTradeID,SK_CustomerID,SK_AccountID,
                                                SK_SecurityID, SK_CompanyID, SK_DateID, SK_TimeID, CurrentPrice,
                                                CurrentHolding, BatchID)
                     SELECT F.HH_H_T_ID, F.HH_T_ID, T.SK_CustomerID, T.SK_AccountID, 
                     T.SK_SecurityID, T.SK_CompanyID, T.SK_CloseDateID, T.SK_CloseTimeID, T.TradePrice, \
                     F.HH_AFTER_QTY, 1
                     FROM s_fact_holding F
                     JOIN DimTrade T ON F.HH_T_ID = T.TradeID;
                   """

        # Construct mysql client bash command to execute ddl and data loading query
        fact_holding_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + fact_holding_ddl + "\""
        fact_holding_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + fact_holding_load_query + "\""

        # Execute the command
        os.system(fact_holding_ddl_cmd)
        os.system(fact_holding_load_cmd)

    def load_target_fact_watches(self):
        """
        create FactWatches table
         """
        # Create ddl to store FactWatches
        fact_watches_ddl = """
               USE """ + self.db_name + """;
        
               CREATE TABLE FactWatches (
                   SK_CustomerID INTEGER NOT NULL,
                   SK_SecurityID INTEGER NOT NULL,
                   SK_DateID_DatePlaced INTEGER NOT NULL,
                   SK_DateID_DateRemoved INTEGER ,
                   BatchID numeric(5) Not NULL, 
                   PRIMARY KEY(SK_CustomerID, SK_SecurityID)
                 );
               """
        fact_watches_load_query = """
                     INSERT INTO FactWatches 
                     SELECT C.CustomerID, S.SK_SecurityID, D.SK_DateID, Null ,1
                     FROM S_Watches W
                     JOIN DimCustomer C ON W.W_C_ID = C.CustomerID
                     JOIN DimSecurity S ON S.Symbol = W.W_S_SYMB
                    JOIN DimDate D on DATE(W.W_DTS) = D.DateValue
                    ON DUPLICATE KEY UPDATE SK_DateID_DateRemoved = D.SK_DateID;
                   """

        # Construct mysql client bash command to execute ddl and data loading query
        fact_watches_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + fact_watches_ddl + "\""
        fact_watches_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + fact_watches_load_query + "\""

        # Execute the command
        os.system(fact_watches_ddl_cmd)
        os.system(fact_watches_load_cmd)


    def load_staging_daily_market(self):
        """
                create staging area for FactMarketHistory table from dailymarket.txt
                 """
        # Create ddl to store FactWatches
        daily_market_ddl = """
                       USE """ + self.db_name + """;

                       CREATE TABLE S_DailyMarketHistory (
                           DM_DATE DATE NOT NULL,
                           DM_S_SYMB char(15) NOT NULL,
                           DM_CLOSE NUMERIC NOT NULL,
                           DM_HIGH NUMERIC ,
                           DM_LOW NUMERIC Not NULL, 
                           DM_VOL NUMERIC 
                         );
                       """
        # Create query to load text data into prospect table
        daily_market_load_query = "LOAD DATA LOCAL INFILE 'staging/" + self.sf + "/Batch1/DailyMarket.txt' INTO " \
                                                                                 "TABLE S_DailyMarketHistory COLUMNS " \
                                                                                 "TERMINATED BY '|';"

        # Construct mysql client bash command to execute ddl and data loading query
        daily_market_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " -D " + self.db_name + " -e \"" + daily_market_ddl + "\""
        daily_market_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD + " --local-infile=1 -D " + self.db_name + " -e \"" + daily_market_load_query + "\""

        # Execute the command
        os.system(daily_market_ddl_cmd)
        os.system(daily_market_load_cmd)
