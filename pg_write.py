import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import csv


if __name__ == '__main__':

    connection = psycopg2.connect(dbname='FitnessTrackerDB', user='postgres', password='1234', host='localhost', port='5432')
    cursor = connection.cursor()
    csv_file = 'Activity.csv'

    cursor.execute("truncate your_table;")

    with open(csv_file, 'r', newline='') as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Пропускаем заголовок CSV файла, если он есть
        for row in csv_data:
            cursor.execute('''
                    INSERT INTO your_table ("UserID", "Date", "Total_Distance", "Tracker_Distance",
                     "Logged_Activities_Distance", "Very_Active_Distance", "Moderately_Active_Distance",
                      "Light_Active_Distance", "Sedentary_Active_Distance", "Very_Active_Minutes",
                       "Fairly_Active_Minutes", "Lightly_Active_Minutes", "Sedentary_Minutes", "Steps", "Calories_Burned")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', row)
        connection.commit()
    print("Successful")






