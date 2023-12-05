# pull_and_dump.py

import os
import argparse
import requests
import pandas as pd
# import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()


def pull_data_from_api(url, api_key, params, timeout=120):
    """
    Extracts football data from an API endpoint using a provided URL and API key.

    Args:
        url (str): The URL of the API endpoint to retrieve data from.
        api_key (str): The API key for authenticating with the API.
        timeout (int, optional): The maximum time (in seconds) to wait for the API
            request to complete. Default is 120 seconds.

    Returns:
        pd.DataFrame or None: A Pandas DataFrame containing the extracted data if the
            API request is successful (status code 200), or None if there was an error
            accessing the data.

    Note:
    - The function sends an HTTP GET request to the specified URL with the provided API key
    in the 'Authorization' header.
    - If the response status code is not 200, an error message is printed, and None is returned.
    - The function assumes the API response is in JSON format and converts it into a Pandas
    DataFrame using 'pd.json_normalize'.
    """
    headers = {'Accept': 'application/json',
               'Authorization': f'Bearer {api_key}'}
    params = {}
    response = requests.get(url, headers=headers,
                            params=params, timeout=timeout)

    if response.status_code == 200:
        df = pd.json_normalize(response.json())
        df.columns = map(str.upper, df.columns)
        return df
    else:
        raise Exception(
            f"Failed to fetch data from API. Status code: {response.status_code}")


def dump_to_snowflake(df, snowflake_conn, table_name):
    """
    Dump a Pandas DataFrame into a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the data to be inserted into Snowflake.
    - snowflake_conn (SnowflakeConnection): An object representing the Snowflake connection details.
    - table_name (str): The name of the Snowflake table where the data will be inserted.

    Returns:
    None

    The function connects to Snowflake using the provided connection details, creates a cursor,
    and inserts the DataFrame records into the specified Snowflake table. The connection is
    committed, and the connection is closed upon completion.

    Example:
    ```python
    # Assuming df, snowflake_conn, and table_name are defined
    dump_to_snowflake(df, snowflake_conn, table_name)
    ```

    Note:
    The SnowflakeConnection object should have the following attributes: login, password, host,
    extra_dejson (a dictionary containing 'warehouse', 'database', and 'schema' keys).
    """

    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        account=snowflake_conn.host,
        warehouse=snowflake_conn.extra_dejson.get('warehouse'),
        database=snowflake_conn.extra_dejson.get('database'),
        schema=snowflake_conn.extra_dejson.get('schema'),
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"USE WAREHOUSE {snowflake_conn.extra_dejson.get('warehouse')}")
            cursor.execute(
                f"USE DATABASE {snowflake_conn.extra_dejson.get('database')}")
            cursor.execute(
                f"USE SCHEMA {snowflake_conn.extra_dejson.get('schema')}")

            write_pandas(conn, df, table_name)
            conn.commit()

    finally:
        conn.close()


def pull_and_dump_data(arg, api_key):
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
    print(f'Query URL: {url}')
    df = pull_data_from_api(url, api_key)

    if arg.get("Export"):
        print('Dumping to Snowflake...')
        snowflake_conn = {
            'login': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASS'),
            'host': os.getenv('SNOWFLAKE_ACCT'),
            'extra_dejson': {
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DB'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            }
        }
        table_name = arg.get("Table", "default_table_name")
        dump_to_snowflake(df, snowflake_conn, table_name)
        return True

    df.to_csv(f'{arg["File"]}.csv')
    return 'Success'


def main(arg, api_key):
    """
    Main function to retrieve data from the College Football Data API, and optionally
    load it into Snowflake data warehouse, for independent testing only. It is 
    not used in the airflow pipeline.

    Args:
        arg (Namespace): Command-line arguments parsed using argparse, containing:
            - Category: The category of data to fetch (e.g., 'games', 'teams', etc.).
            - Search: The search field for filtering data (e.g., 'team', 'year', etc.).
            - Value: The value to search for in the specified category.
        api_key (str): Your API key for authenticating with the College Football Data API.

    Returns:
        bool: True if the data was successfully loaded into Snowflake (if 'to_snowflake'
              is True), otherwise False.

    Note:
    - If 'arg.Export' is True, the function expects the following environment variables
      to be set: SNOWFLAKE_USER, SNOWFLAKE_PASS, SNOWFLAKE_ACCT, SNOWFLAKE_WAREHOUSE,
      SNOWFLAKE_DB, and SNOWFLAKE_SCHEMA for Snowflake connection details.

    - The function calls 'extract_football_from_api' to fetch data from the College
      Football Data API and 'dump_to_snowflake' to load the data into Snowflake.
    """
    url = f'https://api.collegefootballdata.com/{arg.Category}'
    if arg.Search:
        for n, search in enumerate(arg.Search):
            if n == 0:
                url += f'?{search}={arg.Value[n]}'
            else:
                url += f'&{search}={arg.Value[n]}'
    print(f'Query URL: {url}')
    df = pull_data_from_api(url, api_key)

    if arg.Export:
        print('To Snowflake...')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASS')
        account = os.getenv('SNOWFLAKE_ACCT')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DB')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        role = os.getenv('SNOWFLAKE_ROLE')
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )

        success, _, _, _ = write_pandas(
            conn, df, arg.Table, auto_create_table=True)
        conn.close()
        return success

    df.to_csv(f'{arg.File}.csv')
    return 'Success'


if __name__ == '__main__':
    API_KEY = os.getenv("API_KEY")
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--Category',
                        help='Search category', required=True)
    parser.add_argument(
        '-s', '--Search', nargs='+', help='Specific data to search for in the category')
    parser.add_argument('-v', '--Value', nargs='+', help='Subquery value')
    parser.add_argument('-f', '--File', help='Save filename')
    parser.add_argument(
        '-e', '--Export', help='Export query results to Snowflake')
    parser.add_argument('-t', '--Table', help='Snowflake table to save into')
    args = parser.parse_args()
    main(args, API_KEY)