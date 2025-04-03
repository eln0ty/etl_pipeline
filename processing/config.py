# PostgreSQL connection parameters
import os

POSTGRES_CONN_PARAMS = {
    "dbname": "pg-db",
    "user": "eln0ty",
    "password": "admin",
    "host": "localhost",
    "port": "5432",
}

# ETL process configuration
ETL_CONFIG = {
    "batch_size": 1000,
    "file_prefix_requirement": "online",
    "data_archive_path": os.path.join(os.getcwd(), "../data_archive"),
    "version_format": "%Y%m%d_%H%M%S",  # Format for version timestamps
}
