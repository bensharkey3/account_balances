import os
import boto3
import pandas as pd
from datetime import datetime


os.environ["AWS_ACCESS_KEY_ID"] = ''
os.environ["AWS_SECRET_ACCESS_KEY"] = ''

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# s3client = boto3.client('s3')

s3client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
      )


# create cmc df
df_cmc = pd.DataFrame(columns=["snapshot_date","Account Number","Account Name","Code","Last","Currency","FX Rate",
                               "CHESS holdings","Collateral","Recent buys","Recent sells",
                               "Open sells","Cond. sells","Available to sell","Value AUD"])

# get object keys in bucket
bucket_name = 'account-balances-scraper'
s3resource = boto3.resource('s3')
bucket = s3resource.Bucket(bucket_name)

# create cmc df
for obj in bucket.objects.filter(Prefix='cmc-holdings/202'):
    response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
    df_file = pd.read_csv(response.get("Body"))
    date_str = obj.key.split('cmc-holdings/')[1]
    date_str = date_str[0:10]
    df_file['snapshot_date'] = date_str
    # append dfs together
    df_cmc = pd.concat([df_cmc, df_file], axis=0)
    df_cmc.reset_index(drop=True, inplace=True)

# get marketdata df
obj_marketdata = "transformed/marketdata.csv"
response = s3client.get_object(Bucket=bucket_name, Key=obj_marketdata)
df_marketdata = pd.read_csv(response.get("Body"), index_col="Unnamed: 0")

# get summarydata df
obj_summarydata = "transformed/summarydata.csv"
response = s3client.get_object(Bucket=bucket_name, Key=obj_summarydata)
df_summarydata = pd.read_csv(response.get("Body"), index_col="Unnamed: 0")

# clean marketdata df
df_marketdata.drop(['Security Name', 'Security Ratio', 'Security Value'], axis=1, inplace=True)
df_marketdata['Platform'] = 'NAB'

# rename cmc columns to be consistent with marketdata
df_cmc = df_cmc[['Code', 'CHESS holdings', 'Last', 'Value AUD','snapshot_date']]
df_cmc.rename(columns={'CHESS holdings':'Units', 'Last':'Price', 'Value AUD':'Market Value', 'snapshot_date':'File Date'}, inplace=True)
df_cmc['Platform'] = 'CMC'

# concat marketdata and cmc
df_balances = pd.concat([df_cmc, df_marketdata], axis=0)

# get loan value
df_loanvalue = df_summarydata[df_summarydata['Measure'] == 'Loan Value']

# create equity df
df_equity = df_balances.groupby('File Date').sum()[['Market Value']].reset_index()
df_equity = df_equity.merge(df_loanvalue, how='left', on='File Date')[['File Date', 'Market Value', 'Value']]

# cleaning
df_equity.rename(columns={'Value': 'Loan Amount'}, inplace=True)
df_equity['Loan Amount'] = df_equity['Loan Amount'].str.replace('$', '')
df_equity['Loan Amount'] = df_equity['Loan Amount'].str.replace(',', '')
df_equity['Loan Amount'] = df_equity['Loan Amount'].astype(float)
df_equity['File Date'] = pd.to_datetime(df_equity['File Date'])

# add column
df_equity['Equity'] = df_equity['Market Value'] - df_equity['Loan Amount']

# define todays date
todays_date = df_equity.sort_values(by='File Date', ascending=False)['File Date'].iloc[0]

# fill in blanks from weekends, interpolate
all_days = pd.date_range("2022-09-12", todays_date, freq='d').to_frame()
df_equity = pd.merge(all_days, df_equity, how='left', left_on=0, right_on='File Date')
df_equity['File Date'] = df_equity[0]
df_equity.drop(0, axis=1, inplace=True)

df_equity['Equity'].fillna(method='ffill', inplace=True)
df_equity['Loan Amount'].fillna(method='ffill', inplace=True)
df_equity['Market Value'].fillna(method='ffill', inplace=True)
df_equity.sort_values(by='File Date', ascending=False, inplace=True)

# define key dates
yesterdays_date = df_equity.sort_values(by='File Date', ascending=False)['File Date'].iloc[1]
date_7days_ago = df_equity.sort_values(by='File Date', ascending=False)['File Date'].iloc[7]
date_30days_ago = df_equity.sort_values(by='File Date', ascending=False)['File Date'].iloc[30]
