#!/usr/bin/python3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#Dataset locations
equipment = '/home/vagrant/Python_Data_Pipelines/equipment_sensor.csv'
maintenance = '/home/vagrant/Python_Data_Pipelines/maintenance_records.csv'
network = '/home/vagrant/Python_Data_Pipelines/network_sensor.csv'
#Step 1: Extraction: read the data in the 3 CSVs and store as separate DataFrames
def extract_data(file_location):
    df = pd.read_csv(file_location)
    return df

def transform_data(df):
    #column name standardization
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    #remove rows duplicate readings
    df.drop_duplicates(subset=['id', 'date', 'time'],inplace=True)
    #dropping all with missing values
    df.dropna(axis=0, inplace=True)
    # merge date and time columns to create datetime column
    df['datetime'] = df['date'] + ' '+ df['time']
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H',)
    # drop the original date and time columns
    df.drop(['date', 'time'], axis=1, inplace=True)
    return(df)
def agg_sensor_and_maintenance(sensor:pd.DataFrame, maintenance:pd.DataFrame):
    #aggregate
    sensor = sensor.merge(maintenance.loc[:,['equipment_id', 'datetime', 'maintenance_type']], how='left', left_on=['id', 'datetime'], right_on=['equipment_id', 'datetime'])
    sensor = sensor.drop(columns='equipment_id')
    sensor = sensor.fillna({'maintenance_type':'No Maintenance'})
    return sensor
def db_setup()
 #create connection to db
    conn = psycopg2.connect(host='localhost', port=5442, database='maintenance', user='postgres', password='Admin')
    cur = conn.cursor()
    cur.execute('create table if not exists equip_sensor (id integer, datetime date, sensor_reading float8, maintenance_type TEXT) ')
    cur.execute('create table if not exists network_sensor (id integer, datetime date, sensor_reading float8, maintenance_type TEXT)')
    conn.commit()
    cur.close()
    conn.close()
    return 1
def load_data(data_df:pd.DataFrame, table_name):
    #loads data
    engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5442/maintenance')
    data_df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
    return 1
if __name__ == '__main__':
    #extract data
    equipment_df = extract_data(equipment)
    maintenance_df = extract_data(maintenance)
    network_df = extract_data(network)

    # print(equipment_df, '\n', maintenance_df)
    #transform data
    equipment_df = transform_data(equipment_df)
    maintenance_df = transform_data(maintenance_df)
    network_df = transform_data(network_df)

    # print(equipment_df, '\n', maintenance_df)
    #aggregate equipment & sensor data
    merged_equip_df = agg_sensor_and_maintenance(equipment_df, maintenance_df)
    merged_network_df = agg_sensor_and_maintenance(network_df, maintenance_df)

    #check the merged df data
    print(merged_equip_df, '\n')
    print(merged_network_df)
    
    #create the tables in database 'maintenance' for equipment and network sensors data
    database_setup()
    #load the equipment sensor and network sensor data into postgres
    load_data(merged_equip_df, equip_sensor_table)
    load_data(merged_network_df, network_sensor_table)