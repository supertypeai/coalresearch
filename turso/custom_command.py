from dotenv import load_dotenv
from libsql_client import create_client_sync, ResultSet
import os

# Load variables from .env
load_dotenv()

# --- Define Your Custom SQL SELECT Queries Here ---
# Add any SQL query you want to execute and see the results for.
CUSTOM_QUERIES = [
    "SELECT * FROM commodity_report LIMIT 5;",
    "SELECT * FROM mineral_company_report LIMIT 5;",
]


def get_turso_credentials() -> tuple[str, str]:
    """
    Retrieve Turso database URL and auth token from environment variables.
    If not set, print an error message and exit.

    Returns:
        tuple[str, str]: A tuple containing the raw database URL and auth token.
    """
    raw_url = os.getenv("TURSO_DATABASE_URL", "")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    if not raw_url or not auth_token:
        print(
            "Missing Turso credentials. Make sure TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are set in your .env file."
        )
        exit(1)

    return raw_url, auth_token


def normalize_db_url(raw_url: str) -> str:
    """
    Normalize the raw Turso URL to a format suitable for HTTP access.

    Args:
        raw_url (str): The raw Turso database URL.

    Returns:
        str: Normalized URL for HTTP access.
    """
    if raw_url.startswith("wss"):
        db_url = raw_url.replace("wss", "https")
    elif raw_url.startswith("libsql"):
        db_url = raw_url.replace("libsql", "https")
    else:
        db_url = raw_url
    db_url = db_url.rstrip("/")
    return db_url


def print_results_with_headers(results: ResultSet):
    """
    Prints query results in a formatted way with headers, similar to '.headers on'.

    Args:
        results (ResultSet): The ResultSet object from a client.execute() call.
    """
    # Check if there are any results to print
    if not results.rows:
        print("-> No results found.")
        return

    # Print the column headers, separated by " | "
    headers = " | ".join(results.columns)
    print(headers)

    # Print a separator line to distinguish headers from data
    print("-" * len(headers))

    # Print each row of data
    for row in results.rows:
        # Convert each item in the row to a string for printing
        print(" | ".join(map(str, row)))


def main():
    """
    Main function to execute a series of custom SQL queries on a Turso database
    and print the results with headers.
    """
    db_url, auth_token = get_turso_credentials()
    db_url_normalized = normalize_db_url(db_url)
    client = create_client_sync(url=db_url_normalized, auth_token=auth_token)

    print(
        f"Connected to Turso DB. Executing {len(CUSTOM_QUERIES)} custom querie(s)...\n"
    )

    try:
        # Loop through and execute each custom SQL query
        for i, sql_query in enumerate(CUSTOM_QUERIES):
            print(f"--- Running Query #{i + 1}: {sql_query} ---\n")

            # Execute the query
            results = client.execute(sql_query)

            # Print the results with headers
            print_results_with_headers(results)

            print("\n" + "=" * 40 + "\n")  # Add separator for next query result

        print("All custom queries were executed.")
    except Exception as e:
        print(f"An error occurred while executing a query: {e}")
    finally:
        client.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
