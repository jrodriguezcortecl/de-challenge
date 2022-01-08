import psycopg2
from sqlalchemy import create_engine
from config import config
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, regexp_replace, concat_ws, split, col, \
row_number, countDistinct, desc, asc
from pyspark.sql.window import Window

# Main function
def main():
    cur = connect()
    etl(cur)

# Connect to the database for check if the tables exists
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database")
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        check_table(cur,conn,"consoles")
        check_table(cur,conn,"results")

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed")


def check_table(cur,conn,name_table):
    # check if exists table
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (name_table,))
    exists_table = cur.fetchone()[0]

    # if the table doesn't exists created
    if not exists_table:
        if name_table == "consoles":
            command = """
                        CREATE TABLE consoles (
                            console VARCHAR(10) PRIMARY KEY,
                            company VARCHAR(100) NOT NULL
                        )
                        """
        if name_table == "results":
            command = """
                        CREATE TABLE results (
                            name VARCHAR(255) PRIMARY KEY,
                            metascore int NOT NULL,
                            userscore int,
                            date date,
                            console VARCHAR(255) REFERENCES consoles
                        )
                        """

        cur.execute(command)
        conn.commit()
        print("Created table "+str(name_table))
    else:
        print("Table {} exists".format(name_table))

# All ETL process
def etl(cur):
    spark = init()
    df_without_null_userscore, count_companies, df_consoles, df_results  = transform_data(spark)
    top_ten_best_games_by_company(df_without_null_userscore,count_companies)
    top_ten_worst_games_by_company(df_without_null_userscore,count_companies)
    top_ten_best_games(df_without_null_userscore)
    top_ten_worst_games(df_without_null_userscore)
    save_data(cur,'consoles',df_consoles)
    save_data(cur,'results',df_results)

# Init Spark Session
def init():
    return SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "postgresql-42.3.1.jar") \
        .getOrCreate()

# Transform Data
def transform_data(spark):
    # made the transformations to the tables
    df_consoles = transform_consoles(spark)
    df_results = transform_results(spark)
    # join tables to generated the consults
    df = join_dfs(df_consoles,df_results)
    count_companies = return_number_companies(df_consoles)
    return df, count_companies, df_consoles, df_results

# Made the transformations for the df console
def transform_consoles(spark):
    df_consoles = spark.read.csv('data/consoles.csv',inferSchema=True,header=True)
    df_consoles = df_consoles \
            .withColumn("console", trim("console"))
    return df_consoles

# Made the transformations for the df results
def transform_results(spark):
    results = spark.read.csv("data/result.csv",inferSchema=True,header=True)
    df_results = results \
        .withColumn("console", trim("console")) \
        .withColumn("userscore", col("userscore").cast(FloatType()))   \
        .withColumn("month", F.date_format(F.to_date(split(col("date"), " ").getItem(0), "MMM"), "M")) \
        .withColumn("day", regexp_replace(split(col("date"), " ").getItem(1), ",", "")) \
        .withColumn("year", split(col("date"), " ").getItem(2))
    df_results = df_results.withColumn("date",concat_ws("-",col("year"),col("month"),col("day")).cast("date"))
    df_results = df_results.select('name', 'metascore','userscore','date','console')
    return df_results

# Join tables for queries
def join_dfs(df_consoles,df_results):
    return df_results.join(df_consoles, df_results.console == df_consoles.console, "inner")

# Return number companie for future querys
def return_number_companies(df_consoles):
    return df_consoles.select(countDistinct("company")).collect()[0][0]

# Get the 10 best games for each console
def top_ten_best_games_by_company(df,count_companies):
    print('The top 10 best games for each console/company')
    windowDept = Window.partitionBy("company").orderBy(col("metascore").desc())
    df.withColumn("row",row_number().over(windowDept)) \
        .filter(col("row") <= 10) \
        .show(count_companies*10)

# Get the 10 worst games for each console
def top_ten_worst_games_by_company(df,count_companies):
    print('The worst 10 games for each console/company')
    windowDept = Window.partitionBy("company").orderBy(col("metascore").asc())
    df.withColumn("row",row_number().over(windowDept)) \
      .filter(col("row") <= 10) \
      .show(count_companies*10)

# Get the top 10 best games for all consoles
def top_ten_best_games(df):
    print('The top 10 best games for all consoles')
    df.orderBy(desc("metascore")).drop("index").show(10)

# Get the top 10 worst games for all consoles
def top_ten_worst_games(df):
    print('The worst 10 games for all consoles')
    df.orderBy(asc("metascore")).drop("index").show(10)

# Insert dataframe into sql table
def save_data(cur,table_name,df):
    #   Save result to the database
    params = config()
    df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://"+params["host"]+":"+params["port"]+"/"+params["database"]) \
        .option("driver", params["driver_name"]) \
        .option("dbtable",table_name) \
        .option("user",params["user"]) \
        .option("password",params["password"]) \
        .save()
    print("We inserted the data into the table {}".format(table_name))

if __name__ == '__main__':
    main()
