# pull_and_dump.py

from sqlalchemy import create_engine, URL
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def pull_data_from_api(url, api_key, params, timeout=120):
    """
    Extract data from an API and return a Pandas DataFrame.

    Parameters:
    - url (str): The URL of the API endpoint.
    - api_key (str): The API key to use for authentication.
    - params (dict): A dictionary containing the parameters to be passed to the API.
    - timeout (int): The timeout in seconds for the API request.

    Returns:
    - df (pandas.DataFrame): The DataFrame containing the extracted data.

    The function makes a GET request to the specified API endpoint, passing the provided API key,
    parameters, and timeout. If the request is successful, the response is parsed as JSON,
    and the JSON data is converted to a Pandas DataFrame. The DataFrame is returned.

    Example:
    ```python
    # Assuming url, api_key, params, and timeout are defined
    df = pull_data_from_api(url, api_key, params, timeout)
    ```

    Note:
    The timeout parameter is optional and defaults to 120 seconds.
    """

    headers = {"Accept": "application/json", "Authorization": f"Bearer {api_key}"}

    response = requests.get(url, headers=headers, params=params, timeout=timeout)

    if response.status_code == 200:
        df = pd.json_normalize(response.json())
        df.columns = map(str.upper, df.columns)
        return df
    else:
        raise Exception(
            f"Failed to fetch data from API. Status code: {response.status_code}"
        )


def dump_to_csv(df, output_file):
    """
    Dump a Pandas DataFrame to a CSV file.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be dumped to CSV.
    - output_file (str): The path to the output CSV file.

    The function writes the DataFrame to a CSV file using the provided output file path.

    Example:
    ```python
    # Assuming df and output_file are defined
    dump_to_csv(df, output_file)
    ```

    Note:
    This function assumes that the DataFrame has been cleaned and formatted correctly.
    """

    df.to_csv(output_file, index=False)


def dump_to_postgres(df, connect_params, table_name):
    """
    Dump a Pandas DataFrame to a PostgreSQL database.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be dumped to PostgreSQL.
    - connect_params (dict): The connection parameters for the PostgreSQL database.
    - table_name (str): The name of the table to be created in PostgreSQL.

    The function connects to PostgreSQL using the provided connection parameters, creates a table with the
    provided table name, and inserts the DataFrame into the table.

    Example:
    ```python
    # Assuming df, connect_params, and table_name are defined
    dump_to_postgres(df, connect_params, table_name)
    ```

    Note:
    This function assumes that the DataFrame has been cleaned and formatted correctly.
    """

    from sqlalchemy import create_engine

    engine = create_engine(
        f"postgresql://{connect_params['user']}:{connect_params['password']}@{connect_params['host']}:{connect_params['port']}/{connect_params['dbname']}"
    )

    df.to_sql(table_name, engine, if_exists="replace")


def dump_to_bigquery(df, project_id, table_name):
    """
    Dump a Pandas DataFrame to Google BigQuery.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be dumped to BigQuery.
    - project_id (str): The Google BigQuery project ID
    - table_name (str): The name of the table to be created in BigQuery, in the format dataset.tablename.

    The function connects to BigQuery using the provided client, creates a table with the
    provided table_name, and inserts the DataFrame into the table.

    Example:
    ```python
    # Assuming df, bigquery_conn, and table_name are defined
    dump_to_bigquery(df, project_id, table_name)
    ```

    Note:
    This function assumes that the DataFrame has been cleaned and formatted correctly.
    """

    df.to_gbq(table_name, project_id, if_exists="replace")


def dump_to_snowflake(df, snowflake_conn, table_name):
    """
    Dump a Pandas DataFrame to Snowflake.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be dumped to Snowflake.
    - snowflake_conn (sqlalchemy.engine.url.URL): The SQLAlchemy URL for the Snowflake connection.
    - table_name (str): The name of the table to be created in Snowflake.

    The function connects to Snowflake using the provided SQLAlchemy URL, creates a table with the
    provided table name, and inserts the DataFrame into the table.

    Example:
    ```python
    # Assuming df, snowflake_conn, and table_name are defined
    dump_to_snowflake(df, snowflake_conn, table_name)
    ```

    Note:
    This function assumes that the DataFrame has been cleaned and formatted correctly.
    """

    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        account=snowflake_conn.host,
        warehouse=snowflake_conn.extra_dejson.get("warehouse"),
        database=snowflake_conn.extra_dejson.get("database"),
        schema=snowflake_conn.extra_dejson.get("schema"),
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"USE WAREHOUSE {snowflake_conn.extra_dejson.get('warehouse')}"
            )
            cursor.execute(
                f"USE DATABASE {snowflake_conn.extra_dejson.get('database')}"
            )
            cursor.execute(f"USE SCHEMA {snowflake_conn.extra_dejson.get('schema')}")

            write_pandas(conn, df, table_name)
            conn.commit()

    finally:
        conn.close()


def pull_and_dump_data(params, api_key, output):
    """
    Fetches data from the College Football Data API based on the provided arguments
    and optionally exports the data to Snowflake or saves it as a CSV file.

    Parameters:
    - arg (dict): A dictionary containing parameters for querying the API, including:
        - "Category" (str): The category of data to retrieve from the API.
        - "Search" (list): A list of search parameters for filtering the data.
        - "Value" (list): A corresponding list of values for the search parameters.
        - "Export" (bool, optional): If True, the data will be exported to Snowflake.
        - "Table" (str, optional): The name of the Snowflake table to export data to.
        - "File" (str): The base name for the CSV file if not exporting to Snowflake.

    - api_key (str): The API key for accessing the College Football Data API.

    Returns:
    - str or bool: Returns 'Success' if the data is saved as a CSV file. Returns True if the data is
                  successfully exported to Snowflake when the "Export" parameter is set to True.

    Example:
    ```python
    arg = {
        "Category": "teams",
        "Search": ["conference", "year"],
        "Value": ["SEC", 2022],
        "Export": True,
        "Table": "my_table",
        "File": "team_data"
    }
    api_key = "your_api_key"
    result = pull_and_dump_data(arg, api_key)
    print(result)
    ```
    """

    url = f'https://api.collegefootballdata.com/{arg["Category"]}'
    if arg["Search"]:
        for n, search in enumerate(arg["Search"]):
            if n == 0:
                url += f'?{search}={arg["Value"][n]}'
            else:
                url += f'&{search}={arg["Value"][n]}'
    print(f"Query URL: {url}")
    df = pull_data_from_api(url, api_key)

    if arg.get("Export"):
        print("Dumping to Snowflake...")
        snowflake_conn = {
            "login": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASS"),
            "host": os.getenv("SNOWFLAKE_ACCT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        table_name = arg.get("Table", "default_table_name")
        dump_to_snowflake(df, snowflake_conn, table_name)
        return True

    df.to_csv(f'{arg["File"]}.csv')
    return "Success"


if __name__ == "__main__":
    print('My name is main')
