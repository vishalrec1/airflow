import airflow
from airflow import DAG
from datetime import datetime
import json
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(dag_id='load_csv_to_gcs-1',
          start_date=datetime(2023,5,21),
          schedule=None
         )
start = DummyOperator(task_id="start")

def read_json(ti):
    fl = open("/gcp_basic_details.json")
    df = json.load(fl)
    ti.xcom_push(key="PROJECT_ID",value=df['PROJECT_ID'])
    ti.xcom_push(key="BUCKET",value=df['BUCKET'])
    ti.xcom_push(key="SYNTHETIC_DATA_FILE_NM",value=df['SYNTHETIC_DATA_FILE_NM'])
    ti.xcom_push(key="INSTANCE_LIST_FILE_NM",value=df['INSTANCE_LIST_FILE_NM'])

def display_json(ti):
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    print("PROJECT_ID : ",PROJECT_ID)
    print("BUCKET : ",BUCKET)

def create_synthetic_data_csv(ti):
    from faker import Faker
    import os,datetime

    #os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "/gcp-project-0523-628d01f95284.json"
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    csv_file_nm = ti.xcom_pull(key="SYNTHETIC_DATA_FILE_NM",task_ids="read_json")
    instance_list_file_nm = ti.xcom_pull(key="INSTANCE_LIST_FILE_NM",task_ids="read_json")
    print("****PROJECT_ID : ",PROJECT_ID)
    print("****BUCKET : ",BUCKET)
    print("****SYNTHETIC_DATA_FILE_NM : ",csv_file_nm)
    print("****INSTANCE_LIST_FILE_NM : ",instance_list_file_nm)
	
    #Creating the Synthetic Data and Storing it in csv file
    fake = Faker()
    with open('/tmp/'+csv_file_nm, mode='w') as fl:
        print("Start Time is : ",datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        for n in range(250000):
            if n==0:
                header = 'cc_num,cust_num,credit_limit,zip_code,cash_back_pct,ftr_dttm\n'
                print('***HEADER CREATED***')
                fl.write(header)
            else:
                cc_num        = fake.credit_card_number()
                cust_num      = fake.uuid4()
                credit_limit  = fake.numerify(text='%#000')
                zip_code      = fake.zipcode()
                cash_back_pct = fake.numerify(text='%.#')
                ftr_dttm      = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                cc_row        = cc_num+','+cust_num+','+credit_limit+','+zip_code+','+cash_back_pct+','+ftr_dttm+'\n'
                #print(cc_row)
                fl.write(cc_row)
                if n%100000==0:
                    print('The Number of records created are : ',str(n))
               
        print("End Time is : ",datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))

def load_csv_to_gcs_bucket(ti):
    from google.cloud import storage
    import os
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "/gcp-project-0523-628d01f95284.json"
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    csv_file_nm = ti.xcom_pull(key="SYNTHETIC_DATA_FILE_NM",task_ids="read_json")
    print("****PROJECT_ID : ",PROJECT_ID)
    print("****BUCKET : ",BUCKET)
    print("****SYNTHETIC_DATA_FILE_NM : ",csv_file_nm)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name=BUCKET)
    blob = bucket.blob('apache_airflow/'+csv_file_nm)
    blob.upload_from_filename('/tmp/'+csv_file_nm)
    
def create_instance_list_csv_gcs_bucket(ti):
    from google.cloud import storage
    import os
    import pandas as pd
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "/gcp-project-0523-628d01f95284.json"
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    csv_file_nm = ti.xcom_pull(key="SYNTHETIC_DATA_FILE_NM",task_ids="read_json")
    instance_list_fl_nm = ti.xcom_pull(key="INSTANCE_LIST_FILE_NM",task_ids="read_json")
    print("****PROJECT_ID : ",PROJECT_ID)
    print("****BUCKET : ",BUCKET)
    print("****SYNTHETIC_DATA_FILE_NM : ",csv_file_nm)
    print("****INSTANCE_LIST_FILE_NM : ",instance_list_fl_nm)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name=BUCKET)
    blob = bucket.blob('apache_airflow/'+csv_file_nm)
    blob.download_to_filename('/tmp/'+'cc_data_from_gcs.csv')
    df = pd.read_csv('/tmp/'+'cc_data_from_gcs.csv',dtype={'cc_num':'str'})
    df2 = df[['cc_num','ftr_dttm']]
    #Created Read Instance List for Batch Serving to BQ
    df2.to_csv('/tmp/'+instance_list_fl_nm,index=False,header=['creditcard_dtl','timestamp'])
    blob2 = bucket.blob('apache_airflow/'+instance_list_fl_nm)
    #Uploaded Read Instance Lst to GCS Bucket
    blob2.upload_from_filename('/tmp/'+instance_list_fl_nm)
    
def load_cc_data_csv_from_gcs_bucket_to_bq(ti):
    from google.cloud import bigquery
    import os
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "/gcp-project-0523-628d01f95284.json"
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    REGION = ti.xcom_pull(key="REGION",task_ids="read_json")
    csv_file_nm = ti.xcom_pull(key="SYNTHETIC_DATA_FILE_NM",task_ids="read_json")    
    client = bigquery.Client(project=PROJECT_ID,location=REGION)
    job_config = bigquery.LoadJobConfig(
                                    schema=[
                                        bigquery.SchemaField("cc_num", "STRING"),
                                        bigquery.SchemaField("cust_num", "STRING"),
                                        bigquery.SchemaField("credit_limit", "INTEGER"),
                                        bigquery.SchemaField("zip_code", "STRING"),
                                        bigquery.SchemaField("cash_back_pct", "FLOAT"),
                                        bigquery.SchemaField("ftr_dttm", "STRING")
                                    ],
                                    skip_leading_rows=1,
                                    # The source format defaults to CSV, so the line below is optional.
                                    source_format=bigquery.SourceFormat.CSV,
                                )
    uri = 'gs://'+BUCKET+'/apache_airflow/'+csv_file_nm
    table_id = 'gcp-project-0523.cc_dataset.from_gcs_airflow_cc_data'
    load_job = client.load_table_from_uri(source_uris=uri,
                                          destination=table_id,
                                          job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_instance_list_csv_from_gcs_bucket_to_bq(ti):
    from google.cloud import bigquery
    import os
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "/gcp-project-0523-628d01f95284.json"
    PROJECT_ID = ti.xcom_pull(key="PROJECT_ID",task_ids="read_json")
    BUCKET = ti.xcom_pull(key="BUCKET",task_ids="read_json")
    REGION = ti.xcom_pull(key="REGION",task_ids="read_json")
    instance_list_fl_nm = ti.xcom_pull(key="INSTANCE_LIST_FILE_NM",task_ids="read_json")
    client = bigquery.Client(project=PROJECT_ID,location=REGION)
    job_config = bigquery.LoadJobConfig(
                                    schema=[
                                        bigquery.SchemaField("creditcard_dtl", "STRING"),
                                        bigquery.SchemaField("timestamp", "STRING")
                                    ],
                                    skip_leading_rows=1,
                                    # The source format defaults to CSV, so the line below is optional.
                                    source_format=bigquery.SourceFormat.CSV,
                                )
    uri = 'gs://'+BUCKET+'/apache_airflow/'+instance_list_fl_nm
    table_id = 'gcp-project-0523.cc_dataset.from_gcs_airflow_instance_list'
    load_job = client.load_table_from_uri(source_uris=uri,
                                          destination=table_id,
                                          job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
    
read_json = PythonOperator(task_id='read_json',
                           dag=dag,
                           python_callable=read_json
                          )
display_json = PythonOperator(task_id='display_json',
                              dag=dag,
                              python_callable=display_json
                             )
load_csv_to_gcs_bucket = PythonOperator(task_id='load_csv_to_gcs_bucket',
										dag=dag,
									    python_callable=load_csv_to_gcs_bucket
									   )
create_synthetic_data_csv = PythonOperator(task_id='create_synthetic_data_csv',
										  dag=dag,
										  python_callable=create_synthetic_data_csv
										 )
create_instance_list_csv_gcs_bucket = PythonOperator(task_id='create_instance_list_csv_gcs_bucket',
                                                     dag = dag,
                                                     python_callable=create_instance_list_csv_gcs_bucket
                                                    )
load_cc_data_csv_from_gcs_bucket_to_bq = PythonOperator(task_id='load_cc_data_csv_from_gcs_bucket_to_bq',
                                                        dag = dag,
                                                        python_callable=load_cc_data_csv_from_gcs_bucket_to_bq
                                                        )
load_instance_list_csv_from_gcs_bucket_to_bq = PythonOperator(task_id='load_instance_list_csv_from_gcs_bucket_to_bq',
                                                              dag = dag,
                                                              python_callable=load_instance_list_csv_from_gcs_bucket_to_bq
                                                             )
                                               
                                               
start >> read_json >> [display_json,create_synthetic_data_csv] >> load_csv_to_gcs_bucket >> create_instance_list_csv_gcs_bucket >> [load_cc_data_csv_from_gcs_bucket_to_bq,load_instance_list_csv_from_gcs_bucket_to_bq]