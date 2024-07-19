import mysql.connector
from mysql.connector import Error
import pandas as pd
from flask import Flask, render_template

app = Flask(__name__)

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

@app.route('/lol')
def sql_table():
    pw = "Kjsp@123"
    db = "growai"
    connection = create_db_connection("localhost", "root", pw, db)

    if connection is None:
        return "Failed to connect to the database."

    queries = {
        '1.How many given data have been completed Date Wise?': """
            SELECT SUM(`Completed Trips`) AS `End Rides Date wise`
            FROM `all-time trip trends-all cities-nov 01, 2022 → jun 03, 2024`;
        """,
        '2.How many given data have been completed City wise?': """
            select sum(`Completed Trips`) as 'End Rides City wise' from `all-time table-all cities`;
        """,
        '3.How many total searches were conducted Date wise?': """
            select sum(`Searches`) as 'Total Searches Date wise' from `all-time trip trends-all cities-nov 01, 2022 → jun 03, 2024`;
        """,
        '4.How many total searches were conducted City wise?': """
            select sum(`Searches`) as 'Total Searches City wise' from `all-time table-all cities`;
        """,
        '5.How many searches got an estimate ?': """
            select sum(Searches_which_got_estimate) as 'Total Searches Got an Estimate' from `all-time table-all cities`;
        """,
        '6.How many searches were for Quotes ?': """
            select sum(Searches_for_Quotes) as 'Total Searches were for Quotes' from `all-time table-all cities`;
        """,
        '7.How many searches resulted in Quotes ?': """
            select sum(`Searches which got Quotes`) as 'Total Searches resulted in Quotes' from `all-time table-all cities`;
        """,
        '8.How many given data were cancelled by drivers?': """
            select sum(`Cancelled Bookings`) as 'Total Data cancelled by Drivers' from `all-time table-all cities`;
        """,
        '9.How many bookings were cancelled by drivers?': """
            select sum(`Driver Cancellation Rate`) as 'Total Data cancelled by Drivers' from `all-time table-all cities`;
        """,
        '10.How many bookings were not cancelled by drivers?': """
            select (sum(Bookings))-(sum(`Driver Cancellation Rate`)) as 'Total Bookings not Cancelled by Drivers ' from `all-time table-all cities`;
        """,
        '11.How many bookings were not cancelled by Customers?': """
            select sum(`User Cancellation Rate`) as 'Total Data cancelled by Customers' from `all-time table-all cities`;
        """,
        '12.What is the average distance per trip?': """
            select sum(`Average Distance per Trip`) as 'Average Distance per Trip' from `all-time table-all cities`;
        """,
        '13.What is the average fare per trip?': """
            select sum(`Average Fare per Trip`) as 'Average Fare per Trip' from `all-time table-all cities`;
        """,
        '14.What is the total distance travelled?': """
            select sum(`Distance Travelled`) as 'Toatl Distance Travelled' from `all-time table-all cities`;
        """,
        '15.Which two locations had the most given data?': """
            select City from `all-time table-all cities` group by City having MAX(`Completed Trips`) LIMIT 2;
        """,
        '16.During which duration were the most given data taken?': """
            select Time as 'Duration with Highest Trip Count' from `all-time trip trends-all cities-nov 01, 2022 → jun 03, 2024` group by Time having MAX(`Completed Trips`) LIMIT 1;
        """,
        '17.What is the rate of searches to estimate?': """
            select sum(`Search_to_estimate Rate`) as 'End Rides City wise' from `all-time table-all cities`;
        """,
        '18.What is the Quote Acceptance Rate?': """
           select sum(`Driver Quote Acceptance Rate`) as 'End Rides City wise' from `all-time table-all cities`;
        """,
        '19.Which area had the highest number of given data?': """
           select City from `all-time table-all cities` group by City having MAX(`Completed Trips`) LIMIT 1;
        """,
        '20.Which area had the highest cancellation?': """
            select City from `all-time table-all cities` group by City having MAX(`Cancelled Bookings`) LIMIT 1;
        """,
        '21.Which duration had the highest trip count and fares?': """
            SELECT Time AS 'Duration with Highest Trip Count' FROM `all-time trip trends-all cities-nov 01, 2022 → jun 03, 2024` GROUP BY Time 
ORDER BY SUM(`Completed Trips`) DESC, SUM(`Drivers Earnings`) DESC LIMIT 1;
        """,
        '22.What is the booking Cancellation rate?': """
            select sum(`Booking Cancellation Rate`) as 'End Rides City wise' from `all-time table-all cities`;
        """,
    }

    # Execute queries and store results in DataFrames
    data_frames = {}
    try:
        for title, query in queries.items():
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
            data_frames[title] = df
            cursor.close()
    finally:
        # Ensure the connection is closed
        connection.close()

    # Convert DataFrames to HTML tables
    tables = {title: df.to_html(classes='table table-striped', header=True, index=False) for title, df in data_frames.items()}

    return render_template('index.html', tables=tables)

if __name__ == '__main__':
    app.run(port=1234, debug=True)
