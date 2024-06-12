from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(dag_id = 'synthetic_data_local',
          start_date=datetime(2023,5,20),
         )

def create_synthetic_data_csv():
    from faker import Faker
    import os,datetime
    fake = Faker()
    #path = 'D:\\airflow2.6\\'
    file_name = 'cc_data.csv'
    #fl = open(path+file_name,'w')
    fl = open('/opt/airflow/data/'+file_name,'w')

    #Creating the Synthetic Data and Storing it in the GCS Bucket
    print("Start Time is : ",datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    for n in range(10000):
        if n==0:
            header = 'cc_num,cust_num,credit_limit,zip_code,cash_back_pct,ftr_dttm\n'
            fl.write(header)
            print('Header Row added to the File\n')
        else:
            cc_num        = fake.credit_card_number()
            cust_num      = fake.uuid4()
            credit_limit  = fake.numerify(text='%#000')
            zip_code      = fake.zipcode_in_state()
            cash_back_pct = fake.numerify(text='%.#')
            ftr_dttm      = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            cc_row        = cc_num+','+cust_num+','+credit_limit+','+zip_code+','+cash_back_pct+','+ftr_dttm+'\n'
            #print(cc_row)
            fl.write(cc_row)
            if n%1000==0:
                print('The # of records added to the File are : '+str(n))
    print("End Time is : ",datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    fl.close()

def create_zip_cd_agg():
    import pandas as pd
    #path = 'D:\\airflow2.6\\'
    file_name = 'cc_data.csv'
    #df = pd.read_csv(filepath_or_buffer = path+file_name)
    df = pd.read_csv(filepath_or_buffer = '/opt/airflow/data/'+file_name)
    df2 = df.groupby(by=['zip_code'],as_index='False')['zip_code'].count().reset_index(name='counts')
    #df2.to_csv(path_or_buf=path+'loc_agg.csv')
    df2.to_csv(path_or_buf='/opt/airflow/data/'+'loc_agg.csv')

create_synthetic_data_csv = PythonOperator(task_id='create_synthetic_data_csv',
										   dag=dag,
										   python_callable=create_synthetic_data_csv
										  )
create_zip_cd_agg = PythonOperator(task_id='create_zip_cd_agg',
								   dag=dag,
								   python_callable=create_zip_cd_agg
										  )
start = EmptyOperator(task_id="start")
start >> create_synthetic_data_csv >> create_zip_cd_agg