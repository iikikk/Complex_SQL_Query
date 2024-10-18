"""
Transforms and Loads data into the databricks database
"""
from dotenv import load_dotenv
from databricks import sql
import csv
import os

#load the csv file and insert into a new sqlite3 database
def load(dataset="data/Movie_Data.csv"):
    """"Transforms and Loads data into the databricks database"""
    payload = csv.reader(open(dataset, newline=''), delimiter=',')
    next(payload)
    load_dotenv()
    with sql.connect(server_hostname = os.getenv("SERVER_HOSTNAME"),
                 http_path       = os.getenv("HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_KEY")
                 ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS qcmovie 
                           (MovieTitle VARCHAR(255),ReleaseDate DATE,
                           Director VARCHAR(255),Genre VARCHAR(100),
                           BudgetMillions DECIMAL(10, 2),
                           BoxOfficeMillions DECIMAL(10, 2));
                """
            )
            cursor.execute("SELECT * FROM qcmovie")
            result = cursor.fetchall()
            if not result:
                print("here")
                string_sql = "INSERT INTO qcmovie VALUES"
                for i in payload:
                    string_sql += "\n" + str(tuple(i)) + ","
                string_sql = string_sql[:-1] + ";"
                print(string_sql)
                cursor.execute(string_sql)
                # result = cursor.fetchall()

                # for row in result:
                #     print(row)
            cursor.close()
            connection.close()
    return "db loaded or already loaded" 

if __name__ == "__main__":
    load()

