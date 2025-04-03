# Main ETL processor

from config import POSTGRES_CONN_PARAMS, ETL_CONFIG
from URLutils import url_check, download_csv, generate_table_name
from DButils import connect_to_db, create_staging_table, load_into_postgres, table_exists
from TRANSFORMutils import transform_csv, validate_data
from CleanData import clean_data
from DWHdesign import RetailDataWarehouse

class ETLProcessor:
    def __init__(self, conn_params=None, config=None):
        self.conn_params = conn_params or POSTGRES_CONN_PARAMS
        self.config = config or ETL_CONFIG
        self.conn = None
        self.dwh = RetailDataWarehouse(self.conn_params)
        self.dwh.setup_dwh()
    
    def process(self, csv_url):
        try:
            if not url_check(csv_url):
                print("URL is not accessible.")
                return False

            csv_content = download_csv(csv_url)

            self.conn = connect_to_db(self.conn_params)

            table_name = generate_table_name(
                csv_url, 
                prefix_requirement=self.config.get("file_prefix_requirement", "online")
            )
            if not table_exists(self.conn, table_name):
                create_staging_table(self.conn, table_name)

            df = transform_csv(csv_content)
            df = validate_data(df)

            load_into_postgres(self.conn, df, table_name, batch_size=self.config.get("batch_size", 1000))

            df = clean_data()

            self.dwh.load_into_dwh(df)

            return True

        except Exception as e:
            print(f"Error in ETL process: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()
