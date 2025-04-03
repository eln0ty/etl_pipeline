import sys
from ETLprocessor import ETLProcessor

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <csv_url>")
        sys.exit(1)

    csv_url = sys.argv[1]
    processor = ETLProcessor()
    success = processor.process(csv_url)
    
    if success:
        print("ETL process completed successfully.")
    else:
        print("ETL process failed.")
        sys.exit(1)

if __name__ == "__main__":
    #python main.py https://raw.githubusercontent.com/eln0ty/etl_pipeline/refs/heads/main/data-src/online_retail.csv
    main()
