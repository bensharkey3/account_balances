import os
import boto3
import pandas as pd
import numpy as np
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


def convert_value_col(df_col):
    '''change data type from dollars in string to float
    '''
    df_col = df_col.replace({'\$':''}, regex = True)
    df_col = df_col.replace({'\,':''}, regex = True)
    df_col = df_col.astype('float')
    return df_col


# create cmc df
df_cmc = pd.DataFrame(columns=["snapshot_date","Account Number","Account Name","Code","Last","Currency",
                                "FX Rate","CHESS holdings","Collateral","Recent buys","Recent sells",
                                "Open sells","Cond. sells","Available to sell","Value AUD"])

# get object keys in bucket
bucket_name = 'account-balances-scraper'
s3resource = boto3.resource('s3')
bucket = s3resource.Bucket(bucket_name)

# create cmc df from source
for obj in bucket.objects.filter(Prefix='cmc-holdings/202'):
    response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
    df_file = pd.read_csv(response.get("Body"))
    date_str = obj.key.split('cmc-holdings/')[1]
    date_str = date_str[0:10]
    df_file['snapshot_date'] = date_str
    # append dfs together
    df_cmc = pd.concat([df_cmc, df_file], axis=0)
    df_cmc.reset_index(drop=True, inplace=True)

# get marketdata df from source
obj_marketdata = "transformed/marketdata.csv"
response = s3client.get_object(Bucket=bucket_name, Key=obj_marketdata)
df_marketdata = pd.read_csv(response.get("Body"), index_col="Unnamed: 0")

# get summarydata df from source
obj_summarydata = "transformed/summarydata.csv"
response = s3client.get_object(Bucket=bucket_name, Key=obj_summarydata)
df_summarydata = pd.read_csv(response.get("Body"), index_col="Unnamed: 0")

# clean marketdata df
df_marketdata.drop(['Security Name', 'Security Ratio', 'Security Value'], axis=1, inplace=True)
df_marketdata['Platform'] = 'NAB'

# rename cmc columns to be consistent with marketdata
df_cmc = df_cmc[['Code', 'CHESS holdings', 'Last', 'Value AUD','snapshot_date']]
df_cmc.rename(columns={'CHESS holdings':'Units', 'Last':'Price', 'Value AUD':'Market Value', \
    'snapshot_date':'File Date'}, inplace=True)
df_cmc['Platform'] = 'CMC'

# concat marketdata and cmc into one, called unit_holdings
unit_holdings = pd.concat([df_cmc, df_marketdata], axis=0)

# create unit_holdings
unit_holdings = unit_holdings[['File Date', 'Platform', 'Code', 'Units', 'Price', 'Market Value']]\
    .sort_values(by=['File Date', 'Market Value'], ascending=True).reset_index(drop=True)

# add diff columns
unit_holdings['diff_1busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff()
unit_holdings['diff_5busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff(periods=5)
unit_holdings['diff_21busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff(periods=21)
unit_holdings['diff_1busday_marketvalue%'] = 1+ (unit_holdings['diff_1busday_marketvalue'] - \
    unit_holdings['Market Value']) / unit_holdings['Market Value']
unit_holdings['diff_5busday_marketvalue%'] = 1+ (unit_holdings['diff_5busday_marketvalue'] - \
    unit_holdings['Market Value']) / unit_holdings['Market Value']
unit_holdings['diff_21busday_marketvalue%'] = 1+ (unit_holdings['diff_21busday_marketvalue'] - \
    unit_holdings['Market Value']) / unit_holdings['Market Value']

# create bank holdings
bank_holdings = unit_holdings.groupby(['File Date', 'Platform'])[['Market Value']].sum().reset_index()

df_loanvalue = df_summarydata[df_summarydata['Measure'] == 'Loan Value']
df_loanvalue['Value'] = convert_value_col(df_loanvalue['Value'])

bank_holdings = bank_holdings.merge(df_loanvalue, on='File Date')
bank_holdings = bank_holdings[['File Date', 'Platform', 'Market Value', 'Value']]
bank_holdings.rename(columns={'Value': 'Loan Value'}, inplace=True)
bank_holdings['Loan Value'] = np.where(bank_holdings['Platform'] == 'CMC', 0, bank_holdings['Loan Value'])
bank_holdings['Equity'] = bank_holdings['Market Value'] - bank_holdings['Loan Value']

# add diff columns
bank_holdings['diff_1busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff()
bank_holdings['diff_5busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff(periods=5)
bank_holdings['diff_21busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff(periods=21)
bank_holdings['diff_1busday_marketvalue%'] = 1+ (bank_holdings['diff_1busday_marketvalue'] - \
    bank_holdings['Market Value']) / bank_holdings['Market Value']
bank_holdings['diff_5busday_marketvalue%'] = 1+ (bank_holdings['diff_5busday_marketvalue'] - \
    bank_holdings['Market Value']) / bank_holdings['Market Value']
bank_holdings['diff_21busday_marketvalue%'] = 1+ (bank_holdings['diff_21busday_marketvalue'] - \
    bank_holdings['Market Value']) / bank_holdings['Market Value']

bank_holdings['diff_1busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff()
bank_holdings['diff_5busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff(periods=5)
bank_holdings['diff_21busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff(periods=21)
bank_holdings['diff_1busday_equity%'] = 1+ (bank_holdings['diff_1busday_equity'] - \
    bank_holdings['Equity']) / bank_holdings['Equity']
bank_holdings['diff_5busday_equity%'] = 1+ (bank_holdings['diff_5busday_marketvalue'] - \
    bank_holdings['Equity']) / bank_holdings['Equity']
bank_holdings['diff_21busday_equity%'] = 1+ (bank_holdings['diff_21busday_marketvalue'] - \
    bank_holdings['Equity']) / bank_holdings['Equity']

# create total holdings
total_holdings = bank_holdings.groupby('File Date')[['File Date', 'Market Value', 'Loan Value', 'Equity']]\
    .sum().reset_index()

# add diff columns
total_holdings['diff_loanvalue'] = total_holdings['Loan Value'].diff()

total_holdings['diff_1busday_marketvalue'] = total_holdings['Market Value'].diff()
total_holdings['diff_5busday_marketvalue'] = total_holdings['Market Value'].diff(periods=5)
total_holdings['diff_21busday_marketvalue'] = total_holdings['Market Value'].diff(periods=21)
total_holdings['diff_1busday_marketvalue%'] = 1+ (total_holdings['diff_1busday_marketvalue'] - \
    total_holdings['Market Value']) / total_holdings['Market Value']
total_holdings['diff_5busday_marketvalue%'] = 1+ (total_holdings['diff_5busday_marketvalue'] - \
    total_holdings['Market Value']) / total_holdings['Market Value']
total_holdings['diff_21busday_marketvalue%'] = 1+ (total_holdings['diff_21busday_marketvalue'] - \
    total_holdings['Market Value']) / total_holdings['Market Value']

total_holdings['diff_1busday_equity'] = total_holdings['Equity'].diff()
total_holdings['diff_5busday_equity'] = total_holdings['Equity'].diff(periods=5)
total_holdings['diff_21busday_equity'] = total_holdings['Equity'].diff(periods=21)
total_holdings['diff_1busday_equity%'] = 1+ (total_holdings['diff_1busday_equity'] - \
    total_holdings['Equity']) / total_holdings['Equity']
total_holdings['diff_5busday_equity%'] = 1+ (total_holdings['diff_5busday_marketvalue'] - \
    total_holdings['Equity']) / total_holdings['Equity']
total_holdings['diff_21busday_equity%'] = 1+ (total_holdings['diff_21busday_marketvalue'] - \
    total_holdings['Equity']) / total_holdings['Equity']
