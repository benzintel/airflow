import os
from airflow import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import math



def createDirectory(path):
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
        print("The new directory is created!")

def getDataByBranch():
    print("getDataByBranch")
    pathBranchCsv = "/home/airflow/data/"
    pathBranchExcel = '/home/airflow/data/branch/'
    createDirectory(pathBranchExcel)
    for fileName in os.listdir(pathBranchCsv):
        if 'branch_' in fileName:
            name = fileName.split('.')[0]
            df = pd.read_csv(pathBranchCsv + fileName)
            df.to_excel(pathBranchExcel + name + '.xlsx')


def getDataCustomer():
    print("getDataByBranch")
    pathCustomerCsv = '/home/airflow/data/'
    pathCustomerExcel = '/home/airflow/data/customer/'
    createDirectory(pathCustomerExcel)
    dfReadCustomer = pd.read_csv(pathCustomerCsv + 'customers.csv')
    dfReadCustomer.to_excel(pathCustomerExcel + 'customers.xlsx', index=False)


# summaryPointsSpending
def summaryPointsSpending():
    pathBranch = '/home/airflow/data/branch/'
    pathCustomer = '/home/airflow/data/customer/customers.xlsx'
    fileBranchs = []

    dir_list = os.listdir(pathBranch)
    for fileName in dir_list:
        if 'branch_' in fileName:
            fileBranchs.append(pathBranch + fileName)


    dfAllBranch = pd.DataFrame()

    for pathFileName in fileBranchs:
        branchNumber = pathFileName.split('_')[1][:3]
        df = pd.read_excel(pathFileName, engine='openpyxl')
        df['branch'] = branchNumber
        df['spending amount'] = df['spending amount'].astype('float')
        dfAllBranch = pd.concat([dfAllBranch, df])



    spendingAmount = dfAllBranch.groupby('CUSTOMER_ID')['spending amount'].sum().reset_index()
    spendingAmount['Points'] = spendingAmount['spending amount'].apply(lambda x: math.floor(float(x) / 100))

    dfCustomers = pd.read_excel(pathCustomer, engine='openpyxl')
    dfCustomers['Before Point'] = dfCustomers['Points']
    dfCustomersCal = dfCustomers

    dfCustomersAndPoint = dfCustomersCal[["CUSTOMER_ID", "Points"]]
    sumPoints = pd.concat([dfCustomersAndPoint, spendingAmount])
    sumPoints = sumPoints.groupby('CUSTOMER_ID')['Points'].sum().reset_index()

    for coustomerAddPoints in sumPoints['CUSTOMER_ID'].unique():
        addPoint = sumPoints[sumPoints['CUSTOMER_ID'] == coustomerAddPoints]['Points']
        dfCustomersCal.loc[dfCustomersCal['CUSTOMER_ID'] == coustomerAddPoints, 'Points'] = addPoint


    dfCustomersCal['New Points'] = dfCustomersCal['Points'] - dfCustomersCal['Before Point']

    pathExport = '/home/airflow/data/export'
    createDirectory(pathExport)
    with pd.ExcelWriter(pathExport + "/summary_point.xlsx") as writer:
        dfCustomersCal.to_excel(writer, sheet_name="summary", index=False, engine='openpyxl')
        dfAllBranch.to_excel(writer, sheet_name="customer spending", index=False, engine='openpyxl')


default_args = {
    'owner': 'benzintel',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'summary_point_customer',
    default_args=default_args,
    description='Pipeline for ETL summary report data',
    schedule_interval="@daily",
)

t1 = PythonOperator(
    task_id="Get_Spending_By_Branch",
    python_callable=getDataByBranch,
    dag=dag,
)

t2 = PythonOperator(
    task_id="Get_Customer_Information",
    python_callable=getDataCustomer,
    dag=dag,
)

t3 = PythonOperator(
    task_id="Summary_Reports",
    python_callable=summaryPointsSpending,
    dag=dag,
)

t1 >> t2 >> t3