'''
=================================================
Milestone 3

Nama  : Gita Pramoedya Sastri
Batch : RMT-028


=================================================
'''

#Import library 
import datetime as dt
from datetime import timedelta
import psycopg2 as db
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch

#default argument untuk DAG
default_args = {
    'owner': 'gita',
    'start_date': dt.datetime(2024, 3, 23, 10, 12, 0) - dt.timedelta(hours=7), ##nyesuain tanggal dijalaninnya - 7 jam perbedaan dengan UTC
    'catchup': False
}

#fungsi untuk mengambil dataset dari postgres
def fetch_data_from_postgres():
    '''
    Fungsi ini dibuat untuk menjalankan pengambilan data dari PostgresSQL dengan koneksi dari Airflow
    dimana nantinya fungsi ini akan membaca table_m3 dan mengkonversi menjadi format csv untuk dilakukan
    data cleaning 

    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)

    #membaca dan mengambil data dari postgres
    df=pd.read_sql("SELECT * FROM table_m3", conn)
    #menyimpan ke dalam format csv
    df.to_csv('/opt/airflow/dags/P2M3_gita_data_sql.csv')
    #menutup koneksi
    conn.close()

#fungsi untuk mengubah nama kolom
def rename_column(data):
    '''
    Fungsi ini dibuat untuk mengubah nama nama kolom dengan huruf kecil dan mengganti spasi antar nama kolom
    dengan tanda '_' 

    '''
    column_before = data.columns.tolist()
    column_after = [column.lower().replace(' ($)', '').replace(' ', '_') for column in column_before]
    return column_after

#fungsi untuk melakukan handling missing values
def handling_missing(data):
    '''
    Fungsi ini dibuat untuk melakukan handling missing values.

    '''
    data.dropna(inplace=True)
    return data

#fungsi untuk melakukan semua step cleaning
def cleaning_dataset():
    '''
    Fungsi ini dibuat untuk menjalankan semua proses pembersihan data seperti mengubah nama kolom, melakukan handling missing values,
    menghapus data duplikat, mengubah value kolom yang masih mengandung huruf tertentu, mengubah tipe data, dan menyimpannya dalam
    format csv

    '''
    #membaca data yang diambil dari Postgres SQL
    csv_file_path = '/opt/airflow/dags/P2M3_gita_data_sql.csv'
    #membaca data
    dataset = pd.read_csv(csv_file_path)

    #mengubah nama kolom
    new_columns = rename_column(dataset)
    dataset.columns = new_columns

    #menghapus baris yang mengandung missing values
    dataset = handling_missing(dataset)

    #melakukan penghapusan kolom duplikat
    dataset.drop_duplicates(inplace=True)

    #mengubah huruf tertentu dalam kolom 'engine'
    dataset['engine'] = dataset['engine'].replace('Â', '')

    #mengubah tipe data pada kolom date
    dataset['date'] = pd.to_datetime(dataset['date'])
        
    #menyimpan data yang sudah bersih dalam bentuk format csv
    dataset.to_csv('/opt/airflow/dags/P2M3_gita_data_clean.csv', index=False)

#fungsi untuk mempost ke elasticsearch
def post_to_elasticsearch_process(): 
    '''
    Fungsi ini dibuat untuk mengkoneksikan elasticseacrh dengan airflow.
    Kemudian membaca data format csv untuk diubah menjadi format json.

    '''
    es = Elasticsearch('http://elasticsearch:9200') #define the elasticsearch url
    df=pd.read_csv('/opt/airflow/dags/P2M3_gita_data_clean.csv') 
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="ms3", doc_type = "doc", body=doc)
        print(res)


# Define DAG untuk melakukan task
with DAG(
    "project_m3",
    description='Project Milestone 3',
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    
    # Task 1: Fetch data from PostgreSQL
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_postgres,
    )

    # Task 2: for data cleaning
    data_cleaning = PythonOperator(
        task_id='data_cleaning_process',
        python_callable=cleaning_dataset,
    )

    # Task 3: Transport clean CSV into Elasticsearch
    post_to_elasticsearch = PythonOperator(
        task_id='send_to_elasticsearch_task',
        python_callable=post_to_elasticsearch_process,
    )

# Alur task
fetch_data >> data_cleaning >> post_to_elasticsearch
