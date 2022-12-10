import optparse
import configparser
from utils import sort_merge_join, CSV_Transformer
import time

from TPCDI_Loader import TPCDI_Loader

if __name__ == "__main__":
    # Parse user's option
    parser = optparse.OptionParser()
    parser.add_option("-s", "--scalefactor", help="Scale factor used")
    parser.add_option(
        "-d", "--dbname", help="Name of database schema to which the data will be loaded")

    (options, args) = parser.parse_args()

    if not (options.scalefactor and options.dbname):
        parser.print_help()
        exit(1)

    # Read and retrieve config from the configfile
    config = configparser.ConfigParser()
    config.read('db.conf')


    # List all available batches in generated flat files
    # batch_numbers = [int(name[5:]) for name in os.listdir('staging/5') if os.path.isdir(os.path.join('staging/5', name))]
    # But le'ts first work on the first batch
    batch_numbers = [1]
    
    start = time.time()
    for batch_number in batch_numbers:
        # For the historical load, all data are loaded
        if batch_number == 1:
            loader = TPCDI_Loader(options.scalefactor, options.dbname, config, batch_number, overwrite=True)

            # Step 1: Load the batchDate table for this batch
            print("+--------------Going to Load Batch Date--------------------+")
            loader.load_current_batch_date()
            print("+--------------Batch Loading Completed---------------------+")
        
            # Step 2: Load non-dependence tables
            loader.load_dim_date()
            print("+-------------------DimDate Loaded-------------------------+")
            loader.load_dim_time()
            print("+-------------------DimTime Loaded-------------------------+")
            loader.load_industry()
            print("+-------------------Industry Loaded------------------------+")
            loader.load_status_type()
            print("+-------------------StatusType Loaded----------------------+")
            loader.load_tax_rate()
            print("+-------------------TaxRate Loadedd------------------------+")
            loader.load_trade_type()
            print("+-------------------TradeType Loaded-----------------------+")
            loader.load_audit()
            print("+-------------------Audit Loaded---------------------------+")
            loader.init_di_messages()
            print("+---------------DImessage initialized----------------------+")

            # Step 3: Load staging tables
            print("+---------Loading Finware Data to Staging Table------------+")
            loader.load_staging_finwire()
            print("+---------Loading Porspect Data to Staging Table-----------+")
            loader.load_staging_prospect()
            print("+---------Loading Broker Data to Staging Table-------------+")
            loader.load_staging_broker()
            print("+---------Loading Customer Data to Staging Table-----------+")
            loader.load_staging_customer()
            print("+-------Loading CashBalance Data to Staging Table----------+")
            loader.load_staging_cash_balances()
            print("+---------Loading Watches Data to Staging Table------------+")
            loader.load_staging_watches()
            print("+-------Loading FactHolding Data to Staging Table----------+")
            loader.load_staging_fact_holding()

            # Step 4: Load dependant table
            loader.load_target_dim_company()
            print("+-------------------DimCompany Loaded----------------------+")
            loader.load_target_financial()
            print("+-----------------Financial Table Loaded-------------------+")
            loader.load_target_dim_security()
            print("+-------------------DimSecurity Loaded---------------------+")
            loader.load_prospect()
            print("+------------------Prospect Table Loaded-------------------+")
            loader.load_broker()
            print("+-------------------DimBroker Loaded-----------------------+")
            loader.load_target_dim_customer()
            print("+-------------------DimCustomer Loaded---------------------+")
            loader.load_target_dim_account()
            print("+-------------------DimAccount Loaded----------------------+")
            # it has to be loaded after DimAccount
            loader.load_target_fact_cash_balance()
            print("+-----------------FactCashBalance Loaded-------------------+")
            # it has to be loaded after DimTrade
            # loader.load_target_fact_holding()
            # print("+-------------------FactHolding Loaded---------------------+")

    end = time.time()
    print(end-start)
    


    


    