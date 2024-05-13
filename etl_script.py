import clickhouse_connect
import pandas as pd

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='ulejq6zzrh.europe-west4.gcp.clickhouse.cloud',
        user='default',
        password='A0Hd1LXV.Vgr5',
        secure=True
    )
    print("Result:", client.query("select * from \"fitnes-stats\"").result_set)

    csv_file_path = 'Activity.csv'
    df = pd.read_csv(csv_file_path)
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    # df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    print(df['Date'])

    table_name = 'fitnes-stats'

    values = df.values

    print(type(values[0][1]))
    columns = ['UserId','Date','Total_Distance','Tracker_Distance','Logged_Activities_Distance','Very_Active_Distance','Moderately_Active_Distance','Light_Active_Distance','Sedentary_Active_Distance','Very_Active_Minutes','Fairly_Active_Minutes','Lightly_Active_Minutes','Sedentary_Minutes','Steps','Calories_Burned']

    client.insert(table_name, values, columns)

    print("Data inserted successfully.")
    print("Result:", client.query("select * from \"fitnes-stats\"").result_set)