import os
import pandas as pd
import requests
import sqlite3

def download_and_save_data(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"There was error downloading data from {url}: {e}")

def filter_chronic_data(df):
    filtered_df = df[df["DataValueType"] == "Age-adjusted Rate"]
    filtered_df = filtered_df[filtered_df["Demographic"] == "Overall"]
    filtered_df = filtered_df[filtered_df["Question"].str.contains("Mortality", case=False, na=False)]
    deduplicated_df = (
        filtered_df
        .sort_values("Question")
        .groupby(["Year", "State", "Topic"], as_index=False)
        .first()
    )
    return deduplicated_df

def process_and_merge_datasets(chronic_path, nchs_path, selected_columns_chronic, selected_columns_nchs, output_db, output_table, output_csv, clean_columns_chronic, clean_columns_merged, topic_to_cause_mapping):
    nchs_df = pd.read_csv(nchs_path, usecols=selected_columns_nchs, low_memory=False)
    nchs_df.rename(columns={'Cause Name': 'Cause_Name', 'Age-adjusted Death Rate': 'Age_Adjusted_Death_Rate'}, inplace=True)
    relevant_causes = list(topic_to_cause_mapping.values())
    nchs_df = nchs_df[nchs_df['Cause_Name'].isin(relevant_causes)]
    nchs_df = nchs_df.drop_duplicates()

    chronic_df = pd.read_csv(chronic_path, usecols=selected_columns_chronic, low_memory=False)
    chronic_df.rename(columns={
        'YearStart': 'Year',
        'LocationDesc': 'State',
        'DataValue': 'ChronicDiseaseValue',
        'Stratification1': 'Demographic'
    }, inplace=True)

    chronic_df = filter_chronic_data(chronic_df)
    chronic_df['Cause_Name'] = chronic_df['Topic'].map(topic_to_cause_mapping)
    chronic_df = chronic_df.dropna(subset=clean_columns_chronic)

    merged_df = pd.merge(chronic_df, nchs_df, on=['Year', 'State', 'Cause_Name'], how='inner')
    merged_df = merged_df.dropna(subset=clean_columns_merged)
    merged_df = merged_df.drop_duplicates()

    # Remove specified columns
    final_columns = [col for col in merged_df.columns if col not in ['Question', 'Cause_Name', 'Demographic', 'DataValueType']]
    merged_df = merged_df[final_columns]

    conn = sqlite3.connect(output_db)
    schema = """
        Year INTEGER,
        State TEXT,
        Deaths INTEGER,
        Age_Adjusted_Death_Rate REAL,
        Topic TEXT,
        ChronicDiseaseValue REAL,
        GeoLocation TEXT
    """
    conn.execute(f"DROP TABLE IF EXISTS {output_table};")
    conn.execute(f"CREATE TABLE {output_table} ({schema});")
    merged_df.to_sql(output_table, conn, index=False, if_exists='append')
    merged_df.to_csv(output_csv, index=False)
    conn.close()

chronic_path = "../data/downloaded_US_Chronic.csv"
nchs_path = "../data/downloaded_NCHS.csv"
output_db = "../data/processed_data.db"
output_table = "Merged_Data"
output_csv = "../data/merged_data.csv"

US_Chronic_url = "https://data.cdc.gov/api/views/g4ie-h725/rows.csv?accessType=DOWNLOAD"
NCHS_url = "https://data.cdc.gov/api/views/bi63-dtpu/rows.csv?accessType=DOWNLOAD"



selected_columns_chronic = ['YearStart', 'LocationDesc', 'Topic', 'Question', 'DataValue', 'DataValueType', 'Stratification1', 'GeoLocation']
selected_columns_nchs = ['Year', 'State', 'Cause Name', 'Deaths', 'Age-adjusted Death Rate']
clean_columns_chronic = ['Year', 'State', 'Topic', 'ChronicDiseaseValue', 'Cause_Name']
clean_columns_merged = ['Deaths', 'Age_Adjusted_Death_Rate']
topic_to_cause_mapping = {
    "Cancer": "Cancer",
    "Diabetes": "Diabetes",
    "Cardiovascular Disease": "Heart Disease",
    "Chronic Kidney Disease": "Kidney Disease",
    "Immunization": "Influenza and Pneumonia",
    "Chronic Obstructive Pulmonary Disease": "CLRD"
}

download_and_save_data(US_Chronic_url, chronic_path)
download_and_save_data(NCHS_url, nchs_path)

process_and_merge_datasets(
    chronic_path, nchs_path, 
    selected_columns_chronic, selected_columns_nchs, 
    output_db, output_table, 
    output_csv, clean_columns_chronic, clean_columns_merged, 
    topic_to_cause_mapping
)

print("Pipeline execution completed.")
