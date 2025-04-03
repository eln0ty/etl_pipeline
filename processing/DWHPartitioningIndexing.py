class DWHOptimization:
    def __init__(self, conn):
        self.conn = conn
    
    def apply_partitioning(self):
        """
        Apply partitioning to the fact_sales table by year and month.
        This significantly improves query performance for time-based analytics.
        """
        try:
            with self.conn.cursor() as cursor:
                # Create partitioned table structure
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS dwh.fact_sales_partitioned (
                    id SERIAL,
                    product_id INT,
                    customer_id INT,
                    quantity_sold INT,
                    sale_date TIMESTAMP,
                    revenue NUMERIC
                ) PARTITION BY RANGE (sale_date);
                """)
                
                # Create partitions for each month (example shows 2 years)
                years = [2023, 2024, 2025]
                for year in years:
                    for month in range(1, 13):
                        partition_name = f"sales_{year}_{month:02d}"
                        start_date = f"{year}-{month:02d}-01"
                        
                        # Calculate end date
                        if month == 12:
                            end_date = f"{year+1}-01-01"
                        else:
                            end_date = f"{year}-{month+1:02d}-01"
                        
                        cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS dwh.{partition_name}
                        PARTITION OF dwh.fact_sales_partitioned
                        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                        """)
                
                # Create default partition for any data outside specified ranges
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS dwh.sales_default
                PARTITION OF dwh.fact_sales_partitioned DEFAULT;
                """)
                
                self.conn.commit()
                print("=> Partitioning has been applied to fact_sales table")
                return True
        except Exception as e:
            self.conn.rollback()
            print(f"Error applying partitioning: {e}")
            return False
    
    def create_indexes(self):
        """
        Create indexes on dimension and fact tables to optimize query performance.
        """
        try:
            with self.conn.cursor() as cursor:
                # Index for original fact table
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON dwh.fact_sales(sale_date);
                CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON dwh.fact_sales(product_id);
                CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON dwh.fact_sales(customer_id);
                """)
                
                # Indexes for partitioned fact table
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fact_sales_part_product ON dwh.fact_sales_partitioned(product_id);
                CREATE INDEX IF NOT EXISTS idx_fact_sales_part_customer ON dwh.fact_sales_partitioned(customer_id);
                """)
                
                # Indexes for dimension tables
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dim_product_name ON dwh.dim_product(product_name);
                CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dwh.dim_product(category);
                
                CREATE INDEX IF NOT EXISTS idx_dim_customer_location ON dwh.dim_customer(location);
                
                CREATE INDEX IF NOT EXISTS idx_dim_time_year ON dwh.dim_time(year);
                CREATE INDEX IF NOT EXISTS idx_dim_time_month ON dwh.dim_time(month);
                CREATE INDEX IF NOT EXISTS idx_dim_time_quarter ON dwh.dim_time(quarter);
                
                CREATE INDEX IF NOT EXISTS idx_dim_location_name ON dwh.dim_location(country_name);
                """)
                
                self.conn.commit()
                print("=> Indexes have been created on dimension and fact tables")
                return True
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating indexes: {e}")
            return False
    
    def log_data_quality_issue(self, check_type, table_name, issue_description):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO dwh.data_quality_logs (check_type, table_name, issue_description)
                VALUES (%s, %s, %s);
                """, (check_type, table_name, issue_description))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error logging data quality issue: {e}")
    
    def check_sales_data_quality(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT COUNT(*) FROM dwh.fact_sales 
                WHERE quantity_sold <= 0 OR revenue < 0 OR sale_date IS NULL;
                """)
                issue_count = cursor.fetchone()[0]
                if issue_count > 0:
                    self.log_data_quality_issue("Invalid Values", "fact_sales", f"{issue_count} invalid records found")
                    cursor.execute("""
                    INSERT INTO dwh.data_quality_logs (check_type, table_name, issue_description)
                    VALUES ('Auto Check', 'fact_sales', 'Automated data quality check detected issues');
                    """)
                    self.conn.commit()
        except Exception as e:
            print(f"Error checking sales data quality: {e}")
