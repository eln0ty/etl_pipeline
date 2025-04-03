
**Online Retail ETL Pipeline**

This project is an ETL (Extract, Transform, Load) pipeline designed to process online retail data, clean it, transform it, and load it into a data warehouse (DWH) for analysis. The pipeline handles large datasets, such as the provided **online_retail.csv**, and includes scripts for data cleaning, partitioning, indexing, and metadata design.

**Project Overview**

**The Online Retail ETL Pipeline processes raw retail data, performing the following steps:**

1. **Extract**: Reads raw data from CSV files (e.g., **online_retail.csv**).
2. **Transform**: Cleans and transforms the data using scripts like **CleanData.py** and **TRANSFORMUtils.py**.
3. **Load**: Loads the processed data into a data warehouse with scripts like **ETLProcessor.py**, including partitioning and indexing for efficient querying.
4. **Metadata Design**: Generates metadata for the data warehouse using **DWH_Metadata.py**.

The pipeline is containerized using Docker for easy deployment and scalability.

**Prerequisites**

* **Python 3.8 or higher**
* **Docker**

**Installation**

1. **Clone the repository**:
   **bash**

   ```bash
   git clone https://github.com/<your-username>/online-retail-etl-pipeline.git
   cd online-retail-etl-pipeline
   ```
2. **Set up a virtual environment** (optional but recommended):
   **bash**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies**:
   **bash**

   ```bash
   pip install -r requirements.txt
   ```
4. **Set up Docker**:
   If you prefer to run the pipeline in a containerized environment:
   **bash**

   ```bash
   docker-compose up --build
   ```

**Usage**

1. Execute the main script to process the data:
   **bash**

   ```bash
   python main.py <scv_url>
   ```
3. **Debug and analyze**:
   Use the Jupyter notebook in **analysis/debug.csv.ipynb** to explore the data and debug the pipeline.
4. **View the data warehouse design**:
   Check the diagrams in **DWH-Design/** for the data warehouse and metadata design.

**Scripts Overview**

* **CleanData.py**: Cleans raw data by handling missing values, duplicates, and inconsistencies.
* **TRANSFORMUtils.py**: Contains utility functions for data transformation (e.g., normalization, encoding).
* **ETLProcessor.py**: Orchestrates the ETL process, from extraction to loading.
* **DWH_Design.py**: Defines the schema and structure of the data warehouse.
* **DWH_Metadata.py**: Generates metadata for the data warehouse.
* **DWH_PartitioningIndexing.py**: Implements partitioning and indexing for efficient querying.
* **DButils.py**: Provides database connection and query utilities.
* **URLutils.py**: Handles URL-related operations (if applicable).
* **main.py**: Entry point to run the entire pipeline.

**Data**

**The **data-src/** directory is where you should place your raw data for processing.**

**Contributing**

**Contributions are welcome! Please follow these steps:**

1. **Fork the repository.**
2. **Create a new branch (**git checkout -b your-feature**).**
3. **Commit your changes (**git commit -m "Add your feature"**).**
4. **Push to the branch (**git push origin your-feature**).**
5. **Open a pull request.**

**License**

**This project is licensed under the MIT License. See the **LICENSE** file for details.**
