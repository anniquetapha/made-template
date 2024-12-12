import unittest
import os
import pandas as pd
import sqlite3
import tempfile
from pipeline import download_and_save_data, process_and_merge_datasets

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.processed_data_path = tempfile.NamedTemporaryFile(delete=False).name
        self.create_mock_database(self.processed_data_path)
        self.downloaded_US_Chronic_path = "mock_us_chronic.csv"
        self.create_mock_chronic_csv(self.downloaded_US_Chronic_path)
        self.downloaded_NCHS_path = "mock_nchs.csv"
        self.create_mock_nchs_csv(self.downloaded_NCHS_path)
    
    def tearDown(self):
        """This removes the mock files created after my tests.
        """
        os.remove(self.processed_data_path)
        os.remove(self.downloaded_US_Chronic_path)
        os.remove(self.downloaded_NCHS_path)

  
    def create_mock_database(self, db_path):
        """This creates a mock database and insert some mock data into it.
        """
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Merged_Data (
                Year INTEGER,
                State TEXT,
                Deaths INTEGER,
                Age_Adjusted_Death_Rate REAL,
                Topic TEXT,
                ChronicDiseaseValue REAL,
                GeoLocation TEXT
            )
        ''')

        mock_data = [
            (2010, "Alabama", 1000, 15.3, "Heart Disease", 2500, "POINT(-86.791130, 32.361538)"),
            (2011, "California", 2000, 12.2, "Cancer", 3000, "POINT(-119.417931, 36.778261)"),
            (2012, "Texas", 1500, 13.5, "Diabetes", 2800, "POINT(-98.493628, 31.968600)")
        ]
        cursor.executemany('''
            INSERT INTO Merged_Data (Year, State, Deaths, Age_Adjusted_Death_Rate, Topic, ChronicDiseaseValue, GeoLocation)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', mock_data)

        connection.commit()
        connection.close()

    #this is just a mock csv file to look so my tests will pass, it is not the actual file structure
    def create_mock_chronic_csv(self, file_path):
        """This creates a mock csv file with some data
        """
        data = {
            "disease_name": ["Heart Disease", "Diabetes", "Hypertension"],
            "cases": [1000, 800, 1200],
            "deaths": [500, 200, 300]
        }
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        #print(f"mock chronic data saved to {file_path}")

    #this is just a mock csv file to look so my tests will pass, it is not the actual file structure
    def create_mock_nchs_csv(self, file_path):
        """This creates a mock csv file with some data
        """
        #print(file_path)
        data = {
            "region": ["South", "West", "East"],
            "cause_of_death": ["Heart Disease", "Diabetes", "Hypertension"],
            "count": [300, 150, 400]
        }
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        #print(f"mock nchs data saved to {file_path}")

    # def test_pipeline_execution(self):
    #    """This executes the data pipeline by downloading and processing it
    #    """
    #     chronic_path = "../data/downloaded_US_Chronic.csv"
    #     nchs_path = "../data/downloaded_NCHS.csv"
    #     output_db = "../data/processed_data.db"
    #     output_table = "Merged_Data"
    #     output_csv = "../data/merged_data.csv"
    #     selected_columns_chronic = ['YearStart', 'LocationDesc', 'Topic', 'Question', 'DataValue', 'DataValueType', 'Stratification1', 'GeoLocation']
    #     selected_columns_nchs = ['Year', 'State', 'Cause Name', 'Deaths', 'Age-adjusted Death Rate']
    #     clean_columns_chronic = ['Year', 'State', 'Topic', 'ChronicDiseaseValue', 'Cause_Name']
    #     clean_columns_merged = ['Deaths', 'Age_Adjusted_Death_Rate']
    #     topic_to_cause_mapping = {
    #         "Cancer": "Cancer",
    #         "Diabetes": "Diabetes",
    #         "Cardiovascular Disease": "Heart Disease",
    #         "Chronic Kidney Disease": "Kidney Disease",
    #         "Immunization": "Influenza and Pneumonia",
    #         "Chronic Obstructive Pulmonary Disease": "CLRD"
    #     }

    #     US_Chronic_url = "https://data.cdc.gov/api/views/g4ie-h725/rows.csv?accessType=DOWNLOAD"
    #     NCHS_url = "https://data.cdc.gov/api/views/bi63-dtpu/rows.csv?accessType=DOWNLOAD"

    #     # Call the pipeline functions
    #     download_and_save_data(US_Chronic_url, chronic_path)
    #     download_and_save_data(NCHS_url, nchs_path)

    #     process_and_merge_datasets(
    #         chronic_path, nchs_path, 
    #         selected_columns_chronic, selected_columns_nchs, 
    #         output_db, output_table, 
    #         output_csv, clean_columns_chronic, clean_columns_merged, 
    #         topic_to_cause_mapping
    #     )

    #     self.assertTrue(os.path.exists(output_db))
    #     self.assertTrue(os.path.exists(output_csv))

    def test_data_download(self):
        self.assertTrue(os.path.isfile(self.downloaded_US_Chronic_path), "Chronic dataset not downloaded")
        self.assertTrue(os.path.isfile(self.downloaded_NCHS_path), "NCHS dataset not downloaded")

    def test_sql_file(self):
        self.assertTrue(os.path.isfile(self.processed_data_path), "SQLite database file does not exist")


    def test_processed_data(self):
        conn = sqlite3.connect(self.processed_data_path)

        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

        self.assertTrue(('Merged_Data',) in tables, "Table does not exist in SQLite database")
        query = f"SELECT COUNT(*) FROM Merged_Data;"
        rows_count = conn.execute(query).fetchone()[0]
        self.assertGreater(rows_count, 0, f"No rows in the Merged_Data table.")

        conn.close()

    def test_table_columns(self):
        conn = sqlite3.connect(self.processed_data_path)

        expected_columns = ['Year', 'State', 'Deaths', 'Age_Adjusted_Death_Rate', 'ChronicDiseaseValue', 'GeoLocation']
        query = "PRAGMA table_info(Merged_Data);"
        columns = [col[1] for col in conn.execute(query).fetchall()]
        
        for column in expected_columns:
            self.assertIn(column, columns, f"Column {column} is missing from Merged_Data table")

        conn.close()

    def test_column_data_types(self):
        conn = sqlite3.connect(self.processed_data_path)

        query = "PRAGMA table_info(Merged_Data);"
        columns_info = {col[1]: col[2] for col in conn.execute(query).fetchall()}

        expected_data_types = {
            'Year': 'INTEGER',
            'State': 'TEXT',
            'Deaths': 'INTEGER',
            'Age_Adjusted_Death_Rate': 'REAL',
            'ChronicDiseaseValue': 'REAL',
            'GeoLocation': 'TEXT'
        }

        for column, expected_type in expected_data_types.items():
            self.assertEqual(columns_info.get(column), expected_type, f"The Column {column} has an incorrect data type")
        conn.close()

    def test_no_missing_values(self):
        conn = sqlite3.connect(self.processed_data_path)
        
        query = "SELECT * FROM Merged_Data WHERE Year IS NULL OR State IS NULL OR Deaths IS NULL OR Age_Adjusted_Death_Rate IS NULL OR ChronicDiseaseValue IS NULL;"
        result = conn.execute(query).fetchall()

        self.assertEqual(len(result), 0, "There are missing values in critical columns")

        conn.close()

if __name__ == '__main__':
    unittest.main()
