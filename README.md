### Git Issues Analyzer

This is a Python-based command-line tool for scraping and analyzing issues from a GitHub repository. It uses the GitHub API to fetch issue data, processes it with PySpark for scalability, and performs key trend analyses before exporting the results.

-----

### ✨ Features

  * **Scrape Issues:** Fetch all issues (including closed ones) from a specified GitHub repository.
  * **Data Persistence:** Save the scraped issue data to a CSV file for long-term storage and reuse.
  * **Top N Commentators:** Identify and rank the most active commentators in the repository.
  * **Issue Resolution Time:** Calculate the average issue resolution time by month, showing how quickly issues are being addressed.
  * **Month-over-Month Growth:** Analyze the percentage change in new issue creation month-over-month to track project activity.
  * **SQL-based Analysis:** Perform the issue duration analysis using a direct SQL query via PySpark's SQL engine.
  * **All-in-One Command:** Run all analyses sequentially with a single command.

-----

### ⚙️ Prerequisites

This project requires a specific environment due to its use of PySpark and the GitHub API.

1.  **[Python 3.8+](https://www.python.org/)**
2.  **Java 17:** PySpark requires a Java Runtime Environment (JRE).
3.  **[Apache Spark](https://spark.apache.org/downloads.html) installation is mandatory as we are using pyspark:** A local installation of Apache Spark is necessary.
4.  **[GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens):** To avoid API rate limits, you must provide a GitHub token with repository read permissions.
5.  **[Poetry](https://python-poetry.org/):** This project uses Poetry for dependency and environment management.

### 🔧 Installation

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/Ajaykolluri/git-issues.git
    cd git-issues
    ```

2.  **Install Poetry Dependencies:**

    Poetry will automatically create and manage a virtual environment for the project.

    ```bash
    poetry install
    ```

    This command installs `pyspark`, `pandas`, and `PyGithub` as defined in `pyproject.toml`.

3.  **Configure Your GitHub Token:**
     ```bash
        # Example for Linux/macOS
        export GITHUB_TOKEN = "your github personal access token"
     ```
     set this token as an environment variable.

4.  **Configure Apache Spark:**

    Ensure your `SPARK_HOME` environment variable is set to your Spark installation directory.

    ```bash
    # Example for Linux/macOS
    export SPARK_HOME="/path/to/your/spark"
    export PATH="$PATH:$SPARK_HOME/bin"
    ```

-----

### 🚀 Usage

The tool is executed from the command line using `poetry run`. You must use the `--` delimiter to separate your tool's arguments from Poetry's arguments.

**Commands:**

  - `scrape-issues`: Fetches all issues and saves them to a CSV file.
  - `top-n-commenters`: Runs the top commentators analysis.
  - `issue-duration`: Calculates the average issue resolution time.
  - `mom_pert_change`: Calculates the month-over-month issue creation change.
  - `sql-query`: Runs the issue duration analysis using SQL.
  - `all`: Runs all analyses in sequence.

**Example Commands:**

1.  **Scrape issues and save to a CSV:**

    ```bash
    poetry run python main.py --command "scrape-issues" --repo_name "requests/requests" --csvpath "requests_issues.csv"
    ```

2.  **Run all analyses at once:**

    ```bash
    # This command assumes the CSV file from the previous step already exists.
    poetry run python main.py --command "all" --csvpath "requests_issues.csv"
    ```

3.  **Run a specific analysis (e.g., top commenters):**

    ```bash
    poetry run python main.py --command "top-n-commenters" --top_n_commeters 10 --csvpath "requests_issues.csv"
    ```
    
-----

----
**Jupyter file Execution:**

1.  **Go to the path of jupyter file and run the below command :**

    ```bash
    jupyter-notebook
    ```
