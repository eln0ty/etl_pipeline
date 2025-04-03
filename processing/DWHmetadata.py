from DButils import connect_to_db
import pandas as pd

class DWHMetadata:
    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.conn = connect_to_db(self.conn_params)
        self.setup_metadata_schema()
    
    def setup_metadata_schema(self):
        queries = [
            # Create metadata schema
            """
            CREATE SCHEMA IF NOT EXISTS dwh_metadata;
            """,
            
            # Table for storing DWH object information
            """
            CREATE TABLE IF NOT EXISTS dwh_metadata.dwh_objects (
                object_id SERIAL PRIMARY KEY,
                object_name VARCHAR(100) NOT NULL,
                object_type VARCHAR(50) NOT NULL,
                schema_name VARCHAR(50) NOT NULL,
                created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                description TEXT,
                row_count BIGINT,
                UNIQUE(schema_name, object_name)
            );
            """,
            
            # Table for storing column information
            """
            CREATE TABLE IF NOT EXISTS dwh_metadata.column_definitions (
                column_id SERIAL PRIMARY KEY,
                object_id INT NOT NULL REFERENCES dwh_metadata.dwh_objects(object_id),
                column_name VARCHAR(100) NOT NULL,
                data_type VARCHAR(50) NOT NULL,
                is_nullable BOOLEAN NOT NULL,
                is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
                is_foreign_key BOOLEAN NOT NULL DEFAULT FALSE,
                references_table VARCHAR(100),
                references_column VARCHAR(100),
                description TEXT,
                UNIQUE(object_id, column_name)
            );
            """,
            
            # Table for tracking ETL runs
            """
            CREATE TABLE IF NOT EXISTS dwh_metadata.etl_runs (
                run_id SERIAL PRIMARY KEY,
                source_name VARCHAR(200) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                records_processed INT,
                records_inserted INT,
                records_updated INT,
                records_rejected INT,
                error_message TEXT
            );
            """,
            
            # Table for tracking data lineage
            """
            CREATE TABLE IF NOT EXISTS dwh_metadata.data_lineage (
                lineage_id SERIAL PRIMARY KEY,
                target_object_id INT NOT NULL REFERENCES dwh_metadata.dwh_objects(object_id),
                source_object VARCHAR(200) NOT NULL,
                etl_run_id INT REFERENCES dwh_metadata.etl_runs(run_id),
                transformation_logic TEXT,
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
            
            # Table for data quality metrics
            """
            CREATE TABLE IF NOT EXISTS dwh_metadata.data_quality (
                quality_id SERIAL PRIMARY KEY,
                object_id INT NOT NULL REFERENCES dwh_metadata.dwh_objects(object_id),
                check_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                metric_name VARCHAR(100) NOT NULL,
                metric_value NUMERIC,
                threshold NUMERIC,
                pass_fail VARCHAR(5) NOT NULL,
                details TEXT
            );
            """
        ]
        
        with self.conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        
        self.conn.commit()
        print("Metadata schema and tables have been created")
    
    def register_dwh_object(self, object_name, object_type, schema_name, description=None):
        """Register a new DWH object in the metadata repository"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dwh_metadata.dwh_objects (object_name, object_type, schema_name, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (schema_name, object_name) 
                DO UPDATE SET 
                    last_modified = CURRENT_TIMESTAMP,
                    description = EXCLUDED.description
                RETURNING object_id
            """, (object_name, object_type, schema_name, description))
            
            object_id = cursor.fetchone()[0]
        
        self.conn.commit()
        return object_id
    
    def register_column(self, object_id, column_name, data_type, is_nullable, 
                        is_primary_key=False, is_foreign_key=False, 
                        references_table=None, references_column=None, description=None):
        """Register a column for a DWH object"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dwh_metadata.column_definitions 
                (object_id, column_name, data_type, is_nullable, is_primary_key, 
                 is_foreign_key, references_table, references_column, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (object_id, column_name) 
                DO UPDATE SET 
                    data_type = EXCLUDED.data_type,
                    is_nullable = EXCLUDED.is_nullable,
                    is_primary_key = EXCLUDED.is_primary_key,
                    is_foreign_key = EXCLUDED.is_foreign_key,
                    references_table = EXCLUDED.references_table,
                    references_column = EXCLUDED.references_column,
                    description = EXCLUDED.description
                RETURNING column_id
            """, (object_id, column_name, data_type, is_nullable, is_primary_key, 
                  is_foreign_key, references_table, references_column, description))
            
            column_id = cursor.fetchone()[0]
        
        self.conn.commit()
        return column_id
    
    def start_etl_run(self, source_name):
        """Record the start of an ETL run"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dwh_metadata.etl_runs (source_name, start_time, status)
                VALUES (%s, CURRENT_TIMESTAMP, 'RUNNING')
                RETURNING run_id
            """, (source_name,))
            
            run_id = cursor.fetchone()[0]
        
        self.conn.commit()
        return run_id
    
    def complete_etl_run(self, run_id, status, records_processed=0, 
                         records_inserted=0, records_updated=0, records_rejected=0, 
                         error_message=None):
        """Record the completion of an ETL run"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE dwh_metadata.etl_runs
                    SET end_time = CURRENT_TIMESTAMP,
                        status = %s,
                        records_processed = %s,
                        records_inserted = %s,
                        records_updated = %s,
                        records_rejected = %s,
                        error_message = %s
                    WHERE run_id = %s
                """, (status, records_processed, records_inserted, records_updated, 
                      records_rejected, error_message, run_id))
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            print(f"Error updating ETL run: {e}")
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE dwh_metadata.etl_runs
                        SET end_time = CURRENT_TIMESTAMP,
                            status = %s,
                            error_message = %s
                        WHERE run_id = %s
                    """, ("Failed", str(e), run_id))
                self.conn.commit()
            except Exception as rollback_error:
                self.conn.rollback()
                print(f"Error recording failure status: {rollback_error}")

            return None
    
    def record_lineage(self, target_object_id, source_object, etl_run_id, transformation_logic=None):
        """Record data lineage information"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dwh_metadata.data_lineage 
                (target_object_id, source_object, etl_run_id, transformation_logic)
                VALUES (%s, %s, %s, %s)
                RETURNING lineage_id
            """, (target_object_id, source_object, etl_run_id, transformation_logic))
            
            lineage_id = cursor.fetchone()[0]
        
        self.conn.commit()
        return lineage_id
    
    def record_data_quality(self, object_id, metric_name, metric_value, threshold=None, details=None):
        """Record data quality metrics for a DWH object"""
        # Determine if the metric passes the threshold check
        pass_fail = 'PASS'
        if threshold is not None and metric_value < threshold:
            pass_fail = 'FAIL'
        
        # Convert metric_value to float
        metric_value = float(metric_value)

        # Check if metric_value is NaN
        if pd.isna(metric_value):
            print("Error: metric_value is NaN")
            return None

        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO dwh_metadata.data_quality 
                    (object_id, metric_name, metric_value, threshold, pass_fail, details)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING quality_id
                """, (object_id, metric_name, metric_value, threshold, pass_fail, details))

                quality_id = cursor.fetchone()[0]

            self.conn.commit()
            return quality_id

        except Exception as e:
            self.conn.rollback()
            print(f"Error recording data quality: {e}")
            return None
    
    def update_row_count(self, object_id):
        """Update the row count for a DWH object"""
        # First get the schema and table name
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT schema_name, object_name 
                FROM dwh_metadata.dwh_objects 
                WHERE object_id = %s
            """, (object_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Object ID {object_id} not found")
                return
                
            schema_name, object_name = result
            
            # Get the row count
            cursor.execute(f"""
                SELECT COUNT(*) FROM {schema_name}.{object_name}
            """)
            
            row_count = cursor.fetchone()[0]
            
            # Update the object
            cursor.execute("""
                UPDATE dwh_metadata.dwh_objects
                SET row_count = %s,
                    last_modified = CURRENT_TIMESTAMP
                WHERE object_id = %s
            """, (row_count, object_id))
        
        self.conn.commit()
        return row_count
    
    def get_table_metadata(self, schema_name, table_name):
        """Get metadata for a specific table"""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT o.object_id, o.object_name, o.object_type, 
                       o.created_date, o.last_modified, o.description, o.row_count
                FROM dwh_metadata.dwh_objects o
                WHERE o.schema_name = %s AND o.object_name = %s
            """, (schema_name, table_name))
            
            table_info = cursor.fetchone()
            if not table_info:
                return None
                
            columns_query = """
                SELECT c.column_name, c.data_type, c.is_nullable, 
                       c.is_primary_key, c.is_foreign_key, 
                       c.references_table, c.references_column, c.description
                FROM dwh_metadata.column_definitions c
                WHERE c.object_id = %s
                ORDER BY c.column_id
            """
            
            cursor.execute(columns_query, (table_info[0],))
            columns = cursor.fetchall()
            
            lineage_query = """
                SELECT l.source_object, l.transformation_logic, 
                       e.start_time, e.records_processed
                FROM dwh_metadata.data_lineage l
                JOIN dwh_metadata.etl_runs e ON l.etl_run_id = e.run_id
                WHERE l.target_object_id = %s
                ORDER BY e.start_time DESC
            """
            
            cursor.execute(lineage_query, (table_info[0],))
            lineage = cursor.fetchall()
            
            quality_query = """
                SELECT q.check_date, q.metric_name, q.metric_value, 
                       q.threshold, q.pass_fail, q.details
                FROM dwh_metadata.data_quality q
                WHERE q.object_id = %s
                ORDER BY q.check_date DESC
                LIMIT 10
            """
            
            cursor.execute(quality_query, (table_info[0],))
            quality = cursor.fetchall()
        
        # Format the results
        result = {
            'table_info': {
                'object_id': table_info[0],
                'object_name': table_info[1],
                'object_type': table_info[2],
                'created_date': table_info[3],
                'last_modified': table_info[4],
                'description': table_info[5],
                'row_count': table_info[6]
            },
            'columns': [{
                'column_name': col[0],
                'data_type': col[1],
                'is_nullable': col[2],
                'is_primary_key': col[3],
                'is_foreign_key': col[4],
                'references_table': col[5],
                'references_column': col[6],
                'description': col[7]
            } for col in columns],
            'lineage': [{
                'source_object': lin[0],
                'transformation_logic': lin[1],
                'etl_start_time': lin[2],
                'records_processed': lin[3]
            } for lin in lineage],
            'quality': [{
                'check_date': qual[0],
                'metric_name': qual[1],
                'metric_value': qual[2],
                'threshold': qual[3],
                'pass_fail': qual[4],
                'details': qual[5]
            } for qual in quality]
        }
        
        return result
    
    def scan_database_objects(self, schema_name='dwh'):
        """Scan and register all tables and views in a schema"""
        with self.conn.cursor() as cursor:
            # Get all tables and views
            cursor.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (schema_name,))
            
            objects = cursor.fetchall()
            
            for obj_name, obj_type in objects:
                # Register the object
                obj_type_mapped = 'TABLE' if obj_type == 'BASE TABLE' else obj_type
                object_id = self.register_dwh_object(obj_name, obj_type_mapped, schema_name)
                
                # Get columns
                cursor.execute("""
                    SELECT c.column_name, c.data_type, 
                           CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable,
                           CASE WHEN k.column_name IS NOT NULL THEN TRUE ELSE FALSE END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND tc.table_schema = %s
                            AND tc.table_name = %s
                    ) k ON c.column_name = k.column_name
                    WHERE c.table_schema = %s AND c.table_name = %s
                """, (schema_name, obj_name, schema_name, obj_name))
                
                columns = cursor.fetchall()
                
                for col_name, data_type, is_nullable, is_primary_key in columns:
                    self.register_column(
                        object_id, 
                        col_name, 
                        data_type, 
                        is_nullable, 
                        is_primary_key
                    )
                
                # Update row count
                self.update_row_count(object_id)
        
        print(f"Registered {len(objects)} objects from schema '{schema_name}'")
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()


# Example usage
def initialize_dwh_metadata():
    """Initialize the DWH metadata with schema information"""
    from config import POSTGRES_CONN_PARAMS
    
    # Create metadata manager
    # All empty tables has been created with init funtion as constractor
    metadata = DWHMetadata(POSTGRES_CONN_PARAMS)
    
    # Register fact table
    fact_id = metadata.register_dwh_object(
        "fact_sales", 
        "TABLE", 
        "dwh",
        "Main fact table for sales transactions"
    )
    
    # Register fact table columns
    metadata.register_column(
        fact_id, "id", "SERIAL", False, True, False, None, None, "Primary key"
    )
    metadata.register_column(
        fact_id, "product_id", "INTEGER", False, False, True, 
        "dwh.dim_product", "product_id", "Reference to product dimension"
    )
    metadata.register_column(
        fact_id, "customer_id", "INTEGER", False, False, True, 
        "dwh.dim_customer", "customer_id", "Reference to customer dimension"
    )
    metadata.register_column(
        fact_id, "quantity_sold", "INTEGER", False, False, False, 
        None, None, "Quantity of product sold"
    )
    metadata.register_column(
        fact_id, "sale_date", "TIMESTAMP", False, False, False, 
        None, None, "Date of the sale"
    )
    metadata.register_column(
        fact_id, "revenue", "NUMERIC", False, False, False, 
        None, None, "Total revenue from the sale"
    )
    
    # Register product dimension table
    product_id = metadata.register_dwh_object(
        "dim_product", 
        "TABLE", 
        "dwh",
        "Product dimension table"
    )
    
    # Register product dimension columns
    metadata.register_column(
        product_id, "product_id", "SERIAL", False, True, False, 
        None, None, "Primary key"
    )
    metadata.register_column(
        product_id, "product_name", "TEXT", False, False, False, 
        None, None, "Name of the product"
    )
    metadata.register_column(
        product_id, "category", "TEXT", True, False, False, 
        None, None, "Product category"
    )
    
    # Scan for any other objects
    metadata.scan_database_objects()
    
    # Close connection
    metadata.close()
    
    print("DWH metadata initialized successfully")
