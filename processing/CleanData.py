from DButils import connect_to_db
from config import POSTGRES_CONN_PARAMS
import pandas as pd

def clean_data():
    conn = connect_to_db(POSTGRES_CONN_PARAMS)
    cursor = conn.cursor()

    try:
        print("Cleaning data...")

        # Remove rows with NULL in critical columns
        print("=> Removing rows with NULL in critical columns")
        cursor.execute("""
            DELETE FROM online_retail
            WHERE invoiceno IS NULL 
            OR stockcode IS NULL 
            OR quantity IS NULL 
            OR unitprice IS NULL;
        """)
        print(f"   Rows affected: {cursor.rowcount}")

        # Remove negative quantities and unit prices
        print("=> Removing negative quantities and unit prices")
        cursor.execute("""
            DELETE FROM online_retail
            WHERE quantity <= 0 OR unitprice < 0;
        """)
        print(f"   Rows affected: {cursor.rowcount}")

        # Standardize country names
        print("=> Standardizing country names")
        cursor.execute("""
            UPDATE online_retail
            SET country = INITCAP(TRIM(country))
            WHERE country IS NOT NULL;
        """)
        print(f"   Rows affected: {cursor.rowcount}")

        # Fix description formatting
        print("=> Fixing description formatting")
        cursor.execute("""
            UPDATE online_retail
            SET description = TRIM(description)
            WHERE description IS NOT NULL;
        """)
        print(f"   Rows affected: {cursor.rowcount}")

        # Handle duplicate entries while maintaining the primary key
        print("=> Handling duplicate entries while maintaining the primary key")
        cursor.execute("""
            DELETE FROM online_retail
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, 
                           ROW_NUMBER() OVER (PARTITION BY invoiceno, stockcode, quantity, invoicedate ORDER BY id) as row_num
                    FROM online_retail
                ) t
                WHERE t.row_num > 1
            );
        """)
        print(f"   Rows affected: {cursor.rowcount}")

        conn.commit()
        print("Data cleaned successfully.")

        # Retrieve cleaned data and return it as a DataFrame
        print("Fetching cleaned data...")
        cursor.execute("SELECT * FROM online_retail;")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        if df is None:
                print("Error: clean_data() returned None!")
                return False

        return df

    except Exception as e:
        conn.rollback()
        print(f"Error cleaning data: {e}")
        return None  

    finally:
        cursor.close()
        conn.close()
