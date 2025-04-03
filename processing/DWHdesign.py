from datetime import datetime
import pandas as pd
from DButils import connect_to_db
from DWHPartitioningIndexing import DWHOptimization
from DWHmetadata import DWHMetadata

class RetailDataWarehouse:
    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.conn = connect_to_db(self.conn_params)
        self.metadata = DWHMetadata(self.conn_params)

    def create_dwh_schema(self):
        query = """
        CREATE SCHEMA IF NOT EXISTS dwh;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
        self.conn.commit()
        print("=> DWH schema has been created")

    def create_fact_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS dwh.fact_sales (
            id SERIAL PRIMARY KEY,
            product_id INT,
            customer_id INT,
            quantity_sold INT,
            sale_date TIMESTAMP,
            revenue NUMERIC
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
        self.conn.commit()
    
    def create_dimension_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS dwh.dim_product (
                product_id SERIAL PRIMARY KEY,
                product_name TEXT,
                category TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dwh.dim_customer (
                customer_id SERIAL PRIMARY KEY,
                location TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dwh.dim_time (
                time_id SERIAL PRIMARY KEY,
                full_date DATE UNIQUE,
                day INTEGER,
                month INTEGER,
                quarter INTEGER,
                year INTEGER
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dwh.dim_location (
                country_id SERIAL PRIMARY KEY,
                country_name TEXT UNIQUE
            );
            """
        ]

        print("=> Dimension tables have been created")

        with self.conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        self.conn.commit()
    
    def create_data_quality_logs_table(self):
            query = """
                CREATE TABLE IF NOT EXISTS dwh.data_quality_logs (
                    id SERIAL PRIMARY KEY,
                    check_type VARCHAR(255),
                    table_name VARCHAR(255),
                    issue_description TEXT,
                    detected_at TIMESTAMP DEFAULT NOW()
            );
            """
            with self.conn.cursor() as cursor:
                cursor.execute(query)
            self.conn.commit()

        
    
    def setup_dwh(self):
        self.create_dwh_schema()
        self.create_fact_table()
        self.create_dimension_tables()
        self.create_data_quality_logs_table()

        # Initialize metadata after creating tables
        self.metadata.scan_database_objects()

        # Apply optimization
        self.optimization = DWHOptimization(self.conn)
        self.optimization.apply_partitioning()
        self.optimization.create_indexes()
        self.optimization.check_sales_data_quality()

    def load_into_dwh(self, df):
        # Start ETL run and record metadata
        etl_run_id = self.metadata.start_etl_run(f"online_retail_{datetime.now().strftime('%Y%m%d')}")

        records_processed = len(df)
        records_inserted = 0
        records_rejected = 0

        try:
            # Get dimension tables object IDs
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT object_id FROM dwh_metadata.dwh_objects WHERE schema_name = 'dwh' AND object_name = 'fact_sales'")
                fact_id = cursor.fetchone()[0]

                cursor.execute("SELECT object_id FROM dwh_metadata.dwh_objects WHERE schema_name = 'dwh' AND object_name = 'dim_product'")
                product_dim_id = cursor.fetchone()[0]

                cursor.execute("SELECT object_id FROM dwh_metadata.dwh_objects WHERE schema_name = 'dwh' AND object_name = 'dim_customer'")
                customer_dim_id = cursor.fetchone()[0]

            """Load data from a cleaned DataFrame into the data warehouse"""
            # Load dimension tables first
            product_ids = self._load_product_dimension(df)
            customer_ids = self._load_customer_dimension(df)
            time_ids = self._load_time_dimension(df)
            country_ids = self._load_location_dimension(df)

            # Load fact table with the dimension keys
            records_inserted = self._load_fact_table(df, product_ids, customer_ids, time_ids, country_ids)

            records_rejected = records_processed - records_inserted
            print(f"Successfully loaded {records_inserted} records into the data warehouse")

            # Record lineage
            self.metadata.record_lineage(
                fact_id, 
                "online_retail", 
                etl_run_id, 
                "Transformed from source table online_retail via ETL process"
            )
            print("    => Lineage has been recorded")

            # Update row counts
            self.metadata.update_row_count(fact_id)
            self.metadata.update_row_count(product_dim_id)
            self.metadata.update_row_count(customer_dim_id)
            print("    => Row counts have been updated")

            # Record data quality metrics && completeness
            null_count = df.isnull().sum().sum()
            completeness = (1 - (null_count / (df.shape[0] * df.shape[1]))) * 100
            self.metadata.record_data_quality(
                fact_id, 
                "Completeness", 
                float(completeness), 
                95.0, 
                "Percentage of non-null values"
            )
            print("    => Data quality metrics have been recorded")

            # Record duplicate check
            duplicate_count = df.duplicated().sum()
            duplicate_pct = (duplicate_count / df.shape[0]) * 100
            self.metadata.record_data_quality(
                fact_id, 
                "Uniqueness", 
                100 - duplicate_pct, 
                98.0, 
                f"Found {duplicate_count} duplicates in source data"
            )
            print("    => Duplicate check has been recorded")

            # Record ETL completion
            self.metadata.complete_etl_run(
                etl_run_id, 
                "SUCCESS", 
                records_processed, 
                records_inserted, 
                0, 
                records_rejected
            )

            return True

        except Exception as e:
            # Record ETL failure
            self.metadata.complete_etl_run(
                etl_run_id, 
                "FAILED", 
                records_processed, 
                records_inserted, 
                0, 
                records_rejected, 
                str(e)
            )
            print(f"Error in ETL process: {e}")
            # Make sure we rollback if there's an error
            self.conn.rollback()
            return False


    def _load_product_dimension(self, df):
        """Load product dimension and return a mapping of stock codes to product_ids"""
        print("Loading product dimension...")
        product_mapping = {}
        
        with self.conn.cursor() as cursor:
            # Get unique products
            products = df[['stockcode', 'description']].drop_duplicates()
            
            for _, row in products.iterrows():
                # Check if product already exists
                cursor.execute("""
                    SELECT product_id FROM dwh.dim_product
                    WHERE product_name = %s
                """, (row['description'],))
                
                result = cursor.fetchone()
                if result:
                    product_id = result[0]
                else:
                    # Insert new product
                    cursor.execute("""
                        INSERT INTO dwh.dim_product (product_name, category)
                        VALUES (%s, %s)
                        RETURNING product_id
                    """, (row['description'], 'Unknown'))  # Category could be derived if available
                    product_id = cursor.fetchone()[0]
                
                # Store mapping
                product_mapping[row['stockcode']] = product_id
        
        self.conn.commit()
        print(f"  {len(product_mapping)} products loaded")
        return product_mapping
    
    def _load_customer_dimension(self, df):
        """Load customer dimension and return a mapping of customer IDs"""
        print("Loading customer dimension...")
        customer_mapping = {}
        
        with self.conn.cursor() as cursor:
            # Get unique customers
            customers = df[['customerid', 'country']].drop_duplicates()
            
            for _, row in customers.iterrows():
                if pd.isna(row['customerid']):
                    continue  # Skip customers with null ID
                    
                # Check if customer already exists
                cursor.execute("""
                    SELECT customer_id FROM dwh.dim_customer 
                    WHERE customer_id = %s
                """, (int(row['customerid']),))
                
                result = cursor.fetchone()
                if result:
                    customer_id = result[0]
                else:
                    # Insert new customer
                    cursor.execute("""
                        INSERT INTO dwh.dim_customer (customer_id, location)
                        VALUES (%s, %s)
                        RETURNING customer_id
                    """, (int(row['customerid']), row['country']))
                    customer_id = cursor.fetchone()[0]
                
                # Store mapping
                customer_mapping[int(row['customerid'])] = customer_id
        
        self.conn.commit()
        print(f"  {len(customer_mapping)} customers loaded")
        return customer_mapping
    
    def _load_time_dimension(self, df):
        """Load time dimension and return a mapping of dates to time_ids"""
        print("Loading time dimension...")
        time_mapping = {}
        
        with self.conn.cursor() as cursor:
            # Get unique dates
            dates = pd.to_datetime(df['invoicedate']).dt.date.unique()
            
            for date in dates:
                # Check if date already exists
                cursor.execute("""
                    SELECT time_id FROM dwh.dim_time 
                    WHERE full_date = %s
                """, (date,))
                
                result = cursor.fetchone()
                if result:
                    time_id = result[0]
                else:
                    # Convert date to datetime for easier attribute extraction
                    dt = pd.Timestamp(date)
                    
                    # Insert new date
                    cursor.execute("""
                        INSERT INTO dwh.dim_time (full_date, day, month, quarter, year)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING time_id
                    """, (date, dt.day, dt.month, (dt.month - 1) // 3 + 1, dt.year))
                    time_id = cursor.fetchone()[0]
                
                # Store mapping
                time_mapping[date] = time_id
        
        self.conn.commit()
        print(f"  {len(time_mapping)} dates loaded")
        return time_mapping
    
    def _load_location_dimension(self, df):
        print("Loading location dimension...")
        location_mapping = {}
        
        with self.conn.cursor() as cursor:
            # Get unique countries
            countries = df['country'].dropna().unique()
            
            for country in countries:
                # Check if country already exists
                cursor.execute("""
                    SELECT country_id FROM dwh.dim_location 
                    WHERE country_name = %s
                """, (country,))
                
                result = cursor.fetchone()
                if result:
                    country_id = result[0]
                else:
                    # Insert new country
                    cursor.execute("""
                        INSERT INTO dwh.dim_location (country_name)
                        VALUES (%s)
                        RETURNING country_id
                    """, (country,))
                    country_id = cursor.fetchone()[0]
                
                # Store mapping
                location_mapping[country] = country_id
        
        self.conn.commit()
        print(f"  {len(location_mapping)} countries loaded")
        return location_mapping

    def _load_fact_table(self, df, product_ids, customer_ids, time_ids, country_ids):
        """Load data into the partitioned fact table using the dimension mappings"""
        print("Loading fact table...")
        records_loaded = 0

        try:
            with self.conn.cursor() as cursor:
                for _, row in df.iterrows():
                    try:
                        # Skip if missing key dimension data
                        if pd.isna(row['customerid']) or row['stockcode'] not in product_ids:
                            continue

                        # Get dimension keys
                        product_id = product_ids[row['stockcode']]
                        customer_id = customer_ids[int(row['customerid'])]
                        sale_date = pd.to_datetime(row['invoicedate'])
                        date_key = sale_date.date()
                        time_id = time_ids[date_key]

                        # Calculate revenue
                        revenue = row['quantity'] * row['unitprice']

                        # Insert fact record into the partitioned table
                        cursor.execute("""
                            INSERT INTO dwh.fact_sales_partitioned 
                            (product_id, customer_id, quantity_sold, sale_date, revenue)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (product_id, customer_id, row['quantity'], sale_date, revenue))

                        records_loaded += 1

                        # Commit in batches for better performance
                        if records_loaded % 1000 == 0:
                            self.conn.commit()
                            print(f"  {records_loaded} records loaded...")

                    except Exception as e:
                        # Rollback on error for this record
                        self.conn.rollback()
                        print(f"Error loading fact record: {e}")
                        # Start a new transaction
                        continue
                    
            # Final commit for any remaining records
            self.conn.commit()
            print(f"  {records_loaded} total records loaded into partitioned fact table")
            return records_loaded
        except Exception as e:
            self.conn.rollback()
            print(f"Major error in fact table loading: {e}")
            raise