import argparse
import logging
from typing import List, Iterator, Optional

import pandas as pd
from github import Github
from github.Issue import Issue
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, explode, col, avg, date_format, lag, when, to_timestamp, \
    datediff, ceiling, unix_timestamp, lit, count
from pyspark.sql.functions import from_json
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

# Assuming app_cfg and spark are correctly defined in app module
from app import app_cfg, spark

# --- Configure the Logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

git_client = Github(app_cfg.github_token)


def fetch_commenters(issue: Issue, ignore_bot: bool = True) -> List[str]:
    """
    Fetches the usernames of all commentators on a given GitHub issue.

    Args:
        issue: The GitHub Issue object.
        ignore_bot: If True, ignores users with 'bot' in their username.

    Returns:
        A list of usernames who commented on the issue.
    """
    users = []
    try:
        # get_comments() fetches all comments for the issue
        comments = issue.get_comments()
        # Filter out bot users if ignore_bot is True
        if ignore_bot:
            users = [comment.user.login for comment in comments if 'bot' not in comment.user.login]
        else:
            users = [comment.user.login for comment in comments]
        logging.debug(f"Fetched {len(users)} commentators for issue #{issue.number}")
    except Exception as e:
        logging.error(f"Error fetching commentators for issue #{issue.number}: {e}")
    return users


def fetch_issues(repo: str) -> Optional[Iterator[dict]]:
    """
    Fetches issues from a given GitHub repository and formats them.

    Args:
        repo: The name of the repository (e.g., 'owner/repo_name').

    Returns:
        A generator of dictionaries, each representing an issue, or None on failure.
    """
    try:
        # Get the repository object from the GitHub client
        repo_obj = git_client.get_repo(repo)
        # Get all issues (including closed ones)
        issues = repo_obj.get_issues(state="all")
        logging.info(f"Successfully connected to repository '{repo_obj.full_name}'")

        if not issues:
            logging.warning("No issues found in the repository.")
            return None

        # Use a generator for memory efficiency when dealing with a large number of issues
        return ({"issue_no": issue.number,
                 "issue_title": issue.title,
                 "issue_created_by": issue.user.login,
                 "issue_state": issue.state,
                 "created_at": issue.created_at,
                 "closed_at": issue.closed_at,
                 "commenters": fetch_commenters(issue)} for issue in issues)
    except Exception as e:
        logging.error(f"Error fetching issues from repo '{repo}': {e}")
        return None


def save_csv(data: Iterator[dict], file_path: str) -> Optional[int]:
    """
    Saves issue data to a CSV file.

    Args:
        data: A generator of issue dictionaries.
        file_path: The path to the output CSV file.

    Returns:
        The number of issues saved on success, or None on failure.
    """
    try:
        # Convert the generator to a pandas DataFrame
        issues_df = pd.DataFrame(data)
        # Save the DataFrame to a CSV file
        issues_df.to_csv(file_path, index='id', encoding='utf-8')
        logging.info(f"Successfully saved {len(issues_df)} issues to '{file_path}'")
        return len(issues_df)
    except Exception as e:
        logging.error(f"Error saving data to CSV file '{file_path}': {e}")
        return None


def top_commentators(df: DataFrame, top_n_commentators: int = 10) -> Optional[DataFrame]:
    """
    Calculates and returns the top N commentators from a PySpark DataFrame.

    Args:
        df: The PySpark DataFrame with a 'commenters' column.
        top_n_commentators: The number of top commentators to show.

    Returns:
        A DataFrame with top commentators and their counts on success, or None on failure.
    """
    try:
        # Define the schema for the array column
        array_schema = ArrayType(StringType())
        # Convert the string representation of the list to an actual array using from_json
        df_processed = df.withColumn("evaluated_array", from_json(col("commenters"), array_schema))

        # Explode the array to get one row per commentator
        top_ten_names = df_processed.select(explode("evaluated_array").alias("commentator")) \
            .groupBy("commentator") \
            .count() \
            .orderBy(desc("count")) \
            .limit(top_n_commentators)

        logging.info(f"Top {top_n_commentators} commentators calculated.")
        return top_ten_names
    except Exception as e:
        logging.error(f"Error calculating top commentators: {e}")
        return None


def issue_duration(spark_df: DataFrame) -> Optional[DataFrame]:
    """
    Calculates and returns the average issue resolution time by month.

    Args:
        spark_df: The PySpark DataFrame.

    Returns:
        A DataFrame with the average resolution time on success, or None on failure.
    """
    try:
        # Data preparation and duration calculation
        spark_df_processed = (
            spark_df.withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("closed_at", to_timestamp("closed_at", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("resolution_days", datediff("closed_at", "created_at"))
            # Calculate duration in hours using unix_timestamp for precision
            .withColumn("resolution_hours",
                        (unix_timestamp(col("closed_at")) - unix_timestamp(col("created_at"))) / 3600)
            .withColumn("closed_month", date_format("closed_at", "yyyy-MM"))
        )

        logging.info("Calculating average resolution time by month...")
        # Group by the month the issue was closed and calculate the average
        avg_resol_time = (
            spark_df_processed.filter(col("closed_at").isNotNull())
            .groupBy("closed_month")
            # Calculate the average and round to the nearest integer
            .agg(ceiling(avg("resolution_days")).alias("avg_resolution_days"))
            .orderBy("closed_month")
        )
        return avg_resol_time
    except Exception as e:
        logging.error(f"Error calculating issue duration: {e}")
        return None


def per_month_over_month(df: DataFrame) -> Optional[DataFrame]:
    """
    Calculates and returns month-over-month issue creation percentage change.

    Args:
        df: The PySpark DataFrame.

    Returns:
        A DataFrame with the MoM percentage change on success, or None on failure.
    """
    try:
        # Group issues by the month they were created
        df_monthly_issues = (
            df.withColumn("created_month", date_format(col("created_at"), "yyyy-MM"))
            .groupBy("created_month")
            .agg(count(col("issue_no")).alias("current_month_issues"))
            .orderBy("created_month")
        )

        logging.info("Issues Count month-wise calculated.")

        # Define a window to look at the previous row
        window_definition = Window.orderBy("created_month")
        df_with_previous_count = df_monthly_issues.withColumn(
            "previous_month_issues", lag(col("current_month_issues"), 1).over(window_definition)
        )

        logging.info("Monthly counts with previous month's count calculated.")

        # Calculate the month-over-month percentage change
        df_mom_pct_change = df_with_previous_count.withColumn(
            "MoM_Percentage_Change",
            when(col("previous_month_issues").isNull(), lit(None))
            .when(col("previous_month_issues") == 0, lit(None))
            .otherwise(
                ((col("current_month_issues") - col("previous_month_issues")) / col("previous_month_issues")) * 100
            )
        )

        logging.info("Monthly issues with month-over-month Percentage Change calculated.")
        return df_mom_pct_change
    except Exception as e:
        logging.error(f"Error calculating month-over-month percentage change: {e}")
        return None


def issue_duration_sql(df: DataFrame) -> Optional[DataFrame]:
    """
    Calculates and returns the average issue resolution time by month using SQL.

    Args:
        df: The PySpark DataFrame.

    Returns:
        A DataFrame with the average resolution time from the SQL query on success, or None on failure.
    """
    try:
        # Register the DataFrame as a temporary SQL view
        df.createOrReplaceTempView("issues")
        query = """
                SELECT DATE_FORMAT(closed_at, 'yyyy-MM')                                          AS year_month, \
                       CEIL(AVG((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(created_at)) / 3600)) AS avg_closing_hours, \
                       CEIL(AVG(DATEDIFF(closed_at, created_at)))                                 AS avg_closing_days
                FROM issues
                WHERE closed_at IS NOT NULL
                GROUP BY year_month
                ORDER BY year_month \
                """
        logging.info("Running SQL query for issue duration...")
        result_df = spark.sql(query)
        return result_df
    except Exception as e:
        logging.error(f"Error running SQL query for issue duration: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="A program that processes command-line arguments.")
    parser.add_argument("--repo_name", type=str, default="Uniswap/v3-core", help="The name of the github repo.")
    parser.add_argument("--csvpath", type=str, default="./app/gh_issues.csv", help="csvfile name/path")
    parser.add_argument(
        "--command",
        type=str,
        default="all",
        choices=['scrape-issues', 'top-n-commenters', 'issue-duration', 'mom_pert_change', 'sql-query', 'all'],
        help="The task to run. 'all' runs all available tasks."
    )
    args = parser.parse_args()
    logging.info(f"CLI tool started with command: '{args.command}'")

    # --- Centralized Logic to Handle Commands ---
    if args.command == 'scrape-issues':
        issues_data = fetch_issues(args.repo_name)
        if issues_data:
            save_csv(issues_data, args.csvpath)

    # Read the CSV once if it exists
    spark_df = None
    try:
        spark_df = spark.read.csv(args.csvpath, header=True, inferSchema=True)
        logging.info(f"Loaded DataFrame from '{args.csvpath}'. Schema:")
        spark_df.printSchema()
    except Exception as e:
        logging.error(f"Could not read CSV file at '{args.csvpath}'. Please run 'scrape-issues' first. Error: {e}")
        return  # Exit if the DataFrame can't be loaded

    if args.command == 'all':
        logging.info("--- Running All Tasks ---")

        # Capture and display the returned DataFrames
        top_commentators_df = top_commentators(spark_df)
        if top_commentators_df:
            logging.info("Top commentators analysis completed. Showing results:")
            top_commentators_df.show(truncate=False)

        issue_duration_df = issue_duration(spark_df)
        if issue_duration_df:
            logging.info("Issue duration analysis completed. Showing results:")
            issue_duration_df.show()

        mom_pct_change_df = per_month_over_month(spark_df)
        if mom_pct_change_df:
            logging.info("Month-over-month analysis completed. Showing results:")
            mom_pct_change_df.show()

        sql_query_df = issue_duration_sql(spark_df)
        if sql_query_df:
            logging.info("SQL query analysis completed. Showing results:")
            sql_query_df.show()

        logging.info("--- All Tasks Completed ---")
    elif args.command == 'top-n-commenters':
        top_commentators_df = top_commentators(spark_df)
        if top_commentators_df:
            top_commentators_df.show(truncate=False)
    elif args.command == 'issue-duration':
        issue_duration_df = issue_duration(spark_df)
        if issue_duration_df:
            issue_duration_df.show()
    elif args.command == 'mom_pert_change':
        mom_pct_change_df = per_month_over_month(spark_df)
        if mom_pct_change_df:
            mom_pct_change_df.show()
    elif args.command == 'sql-query':
        sql_query_df = issue_duration_sql(spark_df)
        if sql_query_df:
            sql_query_df.show()

    spark.stop()


if __name__ == '__main__':
    main()
    logging.info("Done.")