import psycopg2
from psycopg2 import sql

def connect_to_db(conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Error connecting to the database: {e}")

def table_exists(conn, table_name):
    with conn.cursor() as cursor:
        # Check if the table exists in the database
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f"Table '{table_name}' exists. Truncating...")
            cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(
                sql.Identifier(table_name)
            ))
            print(f"Table '{table_name}' truncated successfully.")
            return True
        else:
            print(f"Table '{table_name}' does not exist. Creating it...")
    return False

def create_staging_table(conn, table_name):
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            invoiceno VARCHAR(50),
            stockcode VARCHAR(50),
            description TEXT,
            quantity BIGINT,
            invoicedate TIMESTAMP,
            unitprice NUMERIC,
            customerid BIGINT,
            country TEXT
        );
    """).format(sql.Identifier(table_name))
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print(f"Table '{table_name}' created.")

    except psycopg2.Error as e:
        raise Exception(f"[X] Error creating table: {e}")

def load_into_postgres(conn, df, table_name, batch_size=1000):
    try:
        total_rows = len(df)
        num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)

        for i in range(num_batches):
            start = i * batch_size
            end = min((i + 1) * batch_size, total_rows)
            batch = df.iloc[start:end]

            data_tuples = [tuple(row) for row in batch.to_numpy()]
            
            insert_query = sql.SQL("""
                INSERT INTO {} ({}) VALUES ({})
            """).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, df.columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(df.columns))
            )

            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data_tuples)
                conn.commit()
                print(f"Inserted batch {i + 1}/{num_batches} ({len(batch)} rows).")

        print(f"Successfully inserted {total_rows} rows into staging table.")
        
    except psycopg2.Error as e:
        conn.rollback()
        if 'data_tuples' in locals():
            for data in data_tuples[:5]:  # Only print the first 5 tuples for debugging
                print(data)
        raise Exception(f"Error inserting data: {e}")
