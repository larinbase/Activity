import psycopg2
import csv
import pandas as pd

def transform_data(data):

    # Удаление пропусков
    data = data.dropna()

    # Удаление дубликатов
    data.drop_duplicates(inplace=True)

    # # Замена пропущенных значений в числовых столбцах средним значением
    # numeric_columns = data.select_dtypes(include='number').columns
    # data[numeric_columns] = data[numeric_columns].fillna(data[numeric_columns].mean())
    #
    # # Замена пропущенных значений в категориальных столбцах модой
    # categorical_columns = data.select_dtypes(include='object').columns
    # data[categorical_columns] = data[categorical_columns].fillna(data[categorical_columns].mode().iloc[0])

    return data

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="FitnessTrackerDB",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

cur.execute("SELECT * FROM your_table")

rows = cur.fetchall()

csv_file_path = "output.csv"

with open(csv_file_path, "w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([desc[0] for desc in cur.description])  # Write column headers
    csv_writer.writerows(rows)

cur.close()
conn.close()

df = pd.read_csv(csv_file_path)
df = df.dropna()
df.to_csv('activity-transform.csv', index=False)

print("Data has been exported to", csv_file_path)
