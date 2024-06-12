from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from airflow.decorators import dag,task

@dag(schedule=None,
     start_date=datetime(2023,5,20),
	)
def synthetic_data_taskflow():
	@task()
	def create_synthetic_data_csv():
		from faker import Faker
		import os,datetime
		fake = Faker()
		ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
		file_name = 'cc_data'+ts+'.csv'
		fl = open('/opt/airflow/data/'+file_name,'w')

		#Creating the Synthetic Data and Storing it in the GCS Bucket
		print("Start Time is : ",datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
		for n in range(100000):
			if n==0:
				header = 'cc_num,cust_num,credit_limit,zip_code,cash_back_pct,ftr_dttm\n'
				fl.write(header)
				print('Header Row added to the File\n')
			else:
				cc_num        = fake.credit_card_number()
				cust_num      = fake.uuid4()
				credit_limit  = fake.numerify(text='h%#000')
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
		return file_name

	@task()
	def create_zip_cd_agg(file_name):
		import pandas as pd
		import datetime
		#file_name = 'cc_data.csv'
		ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
		df = pd.read_csv(filepath_or_buffer = '/opt/airflow/data/'+file_name)
		df2 = df.groupby(by=['zip_code'],as_index='False')['zip_code'].count().reset_index(name='counts')
		df2.to_csv(path_or_buf='/opt/airflow/data/'+'loc_agg'+ts+'.csv')

	create_zip_cd_agg(create_synthetic_data_csv())

synthetic_data_taskflow()