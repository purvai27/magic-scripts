"""
Author: Hevo
File  : empty_column_generator.py

Purpose:
--------
This script demonstrates a simple Python program that performs basic operation to
generate empty column objects.

Usage Documentation:
------
https://api-docs.hevodata.com/reference/introduction


License:
--------
This script has no license. It is provided "as-is" without any warranty. Feel free to use
and modify it for any purpose.
"""

import mysql.connector
from google.cloud import bigquery
from google.oauth2 import service_account

def get_mysql_connection():   #Add your connection detials
    return mysql.connector.connect(
        user='<user>',
        password='<db_password>'
        host='<db_host>',
        database='<db_name>'
    )

def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        r'C:/Users/Snehansu/Downloads/<hevo-test-266608-f5a5e296518f.json>' #add path of your JSON file
    )
    return bigquery.Client(project='<hevo-test-266608>', credentials=credentials)

def get_columns(cursor, table_name):
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    return [row[0].upper() for row in cursor.fetchall()]

def get_columns_bigquery(client, dataset_name, table_name):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)
    return [schema_field.name.upper() for schema_field in table.schema]

def add_columns_to_bigquery(client, dataset_name, table_name, columns):
    table = client.get_table(f"{dataset_name}.{table_name}")
    existing_columns = [field.name for field in table.schema]
    new_schema = table.schema[:]

    for column in columns:
        if column not in existing_columns:
            new_schema.append(bigquery.SchemaField(column, "STRING"))
    table.schema = new_schema
    client.update_table(table, ["schema"])
    print(f"Updated schema of BigQuery table {table_name} with new columns: {columns}")

def main():
    # Connect to MySQL
    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    # Connect to BigQuery
    bq_client = get_bigquery_client()

    # Table and dataset names
    mysql_table_name = 'orders_summary'
    bq_dataset_name = 'hevo_dataset_hevo_test_266608_Py5H'
    bq_table_name = 'harman_orders_summary'

    # Get columns from MySQL
    mysql_columns = get_columns(mysql_cursor, mysql_table_name)
    print(f"Retrieved MySQL columns: {mysql_columns}")

    # Get columns from BigQuery
    bq_columns = get_columns_bigquery(bq_client, bq_dataset_name, bq_table_name)
    print(f"Retrieved BigQuery columns: {bq_columns}")

    # Find columns present in MySQL but not in BigQuery
    missing_columns = list(set(mysql_columns) - set(bq_columns))
    print(f"Missing columns in BigQuery: {missing_columns}")

    if missing_columns:
        # Add missing columns to BigQuery
        add_columns_to_bigquery(bq_client, bq_dataset_name, bq_table_name, missing_columns)

    # Close connections
    mysql_cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    main()
