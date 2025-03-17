# CSV_to_DB_exporter
## Description
This script imports data from CSV files into an Oracle database. It automatically creates tables based on the structure of the CSV files and inserts the data into the corresponding tables. The program ensures that the proper column types are used during the creation of tables. It is designed to handle large datasets efficiently and can generate SQL queries for creating tables in Oracle based on the CSV file content.

## Functional Description
The program performs the following steps:
1. Reads CSV files with the provided path.
2. Creates a table in the Oracle database based on the columns of the CSV file.
3. Maps CSV column types to Oracle types and generates the corresponding SQL for table creation.
4. Inserts data from the CSV file into the Oracle database table.
5. Handles memory management efficiently to avoid excessive memory usage during the import.

## How It Works
1. The script first connects to an Oracle database using SQLAlchemy.
2. The CSV file is read in chunks for memory efficiency.
3. The script generates the appropriate table creation SQL statements based on the column names and data types from the CSV file.
4. The data is inserted into the Oracle database using an insert statement.
5. After the import, the script ensures that resources are released, and memory usage is optimized.

## Input Structure
To run the program, the following parameters need to be provided:
1. **CSV File Paths**: Paths to the CSV files that need to be imported.
2. **Oracle Database Credentials**: Username, Password, and Data Source Name (DSN) for the Oracle database.
3. **Column Information**: The script automatically handles column names and types based on the CSV content.

## Technical Requirements
To run the program, the following are required:
1. Python 3.x
2. Installed libraries:
   - sqlalchemy
   - pandas
   - oracledb (cx_Oracle)
3. An Oracle Database to connect to, with necessary credentials (username, password, DSN).

## Usage
1. Modify the `username`, `password`, and `dsn` values in the script to connect to your Oracle database.
2. Set the paths to the CSV files that you want to import.
3. Run the script, and it will:
   - Read the CSV file(s).
   - Automatically create tables in the Oracle database based on the CSV structure.
   - Insert the CSV data into the corresponding tables.

## Example Output
- SQL statements for creating tables:
   - The script generates SQL `CREATE TABLE` statements based on CSV columns and their types.
- Insert statements:
   - Data from the CSV file is inserted into the corresponding Oracle table.

## Conclusion
This script simplifies the process of importing CSV files into an Oracle database, automatically generating the necessary tables and inserting data. It ensures that the data structure matches between the CSV file and the Oracle database, making the import process seamless and efficient.
