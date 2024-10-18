"""Query the database"""
from dotenv import load_dotenv
from databricks import sql
import os

complex_query = """
WITH Director_Finances AS (
    SELECT
        Director,
        COUNT(MovieTitle) AS Movies_Directed,
        AVG(BudgetMillions) AS Avg_Budget,
        AVG(BoxOfficeMillions) AS Avg_Box_Office,
        SUM(BoxOfficeMillions) - SUM(BudgetMillions) AS Profit
    FROM default.qcmovie
    GROUP BY Director
)

SELECT
    M.MovieTitle,
    M.ReleaseDate,
    M.Director,
    M.Genre,
    M.BudgetMillions,
    M.BoxOfficeMillions,
    D.Movies_Directed,
    D.Avg_Budget,
    D.Avg_Box_Office,
    D.Profit
FROM default.qcmovie M
JOIN Director_Finances D ON M.Director = D.Director
ORDER BY D.Profit DESC, M.BoxOfficeMillions DESC;
"""
def query():
    """Query the database for the top 5 rows of the GroceryDB table"""
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                 http_path       = os.getenv("HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_KEY")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(complex_query)
            result = cursor.fetchall()

            for row in result:
                print(row)
            cursor.close()
            connection.close()
    return "query successful"
    
if __name__ == "__main__":
    query()

