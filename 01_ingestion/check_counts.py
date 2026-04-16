import pandas as pd

from scripts.ml_clustering import init_snowflake_connection


def check_data():
    conn = init_snowflake_connection()

    query_raw = "SELECT count(*) as total FROM SILVER.FCT_CARS"
    df_raw = pd.read_sql(query_raw, conn)
    print("Total rows in SILVER.FCT_CARS: ", df_raw["TOTAL"][0])

    query_valid = """
        SELECT count(*) as valid FROM SILVER.FCT_CARS
        WHERE price IS NOT NULL 
          AND mileage IS NOT NULL
          AND year IS NOT NULL
    """
    df_valid = pd.read_sql(query_valid, conn)
    print(
        "Rows valid for ML training (not null price/mileage/year): ",
        df_valid["VALID"][0],
    )

    query_clusters = "SELECT CLUSTER_NAME, count(*) as count FROM GOLD.MART_MARKET_CLUSTERS GROUP BY CLUSTER_NAME"
    try:
        df_clusters = pd.read_sql(query_clusters, conn)
        print("\nCluster Distribution:")
        print(df_clusters)
    except Exception as e:
        print("\nCould not read clusters:", e)


if __name__ == "__main__":
    check_data()
