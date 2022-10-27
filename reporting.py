import os
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


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


def convert_percent_col(df_col):
    '''change data type from percent in string to float
    '''
    df_col = df_col.replace({'\%':''}, regex = True)
    df_col = df_col.replace({'\,':''}, regex = True)
    df_col = df_col.astype('float')
    return df_col


def as_currency(amount):
    if amount >= 0:
        return "${:,.0f}".format(amount)
    else:
        return "-${:,.0f}".format(-amount)

def units_message_str(code, units, units_change):
    units_message = f"Units added today  {code}:  {units} (+{units_change})"
    return units_message


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

# get loandetails df from source
obj_loandetails = "transformed/loandetails.csv"
response = s3client.get_object(Bucket=bucket_name, Key=obj_loandetails)
df_loandetails = pd.read_csv(response.get("Body"), index_col="Unnamed: 0")

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

# clean df to remove unit holdings = 0
unit_holdings = unit_holdings[unit_holdings['Units'] != 0]

# add unit_holdings diff columns
unit_holdings['diff_1busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff()
unit_holdings['diff_5busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff(periods=5)
unit_holdings['diff_21busday_marketvalue'] = unit_holdings.groupby('Code')['Market Value'].diff(periods=21)

unit_holdings['diff_1busday_units'] = unit_holdings.groupby('Code')['Units'].diff()
unit_holdings['diff_5busday_units'] = unit_holdings.groupby('Code')['Units'].diff(periods=5)
unit_holdings['diff_21busday_units'] = unit_holdings.groupby('Code')['Units'].diff(periods=21)

unit_holdings['diff_1busday_marketvalue%'] = unit_holdings['diff_1busday_marketvalue'] / \
    (unit_holdings['Market Value'] - unit_holdings['diff_1busday_marketvalue'])
unit_holdings['diff_5busday_marketvalue%'] = unit_holdings['diff_5busday_marketvalue'] / \
    (unit_holdings['Market Value'] - unit_holdings['diff_5busday_marketvalue'])
unit_holdings['diff_21busday_marketvalue%'] = unit_holdings['diff_21busday_marketvalue'] / \
    (unit_holdings['Market Value'] - unit_holdings['diff_21busday_marketvalue'])

unit_holdings['diff_1busday_units%'] = unit_holdings['diff_1busday_units'] / \
    (unit_holdings['Units'] - unit_holdings['diff_1busday_units'])
unit_holdings['diff_5busday_units%'] = unit_holdings['diff_5busday_units'] / \
    (unit_holdings['Units'] - unit_holdings['diff_5busday_units'])
unit_holdings['diff_21busday_units%'] = unit_holdings['diff_21busday_units'] / \
    (unit_holdings['Units'] - unit_holdings['diff_21busday_units'])

# create bank holdings
bank_holdings = unit_holdings.groupby(['File Date', 'Platform'])[['Market Value']].sum().reset_index()

df_loanvalue = df_summarydata[df_summarydata['Measure'] == 'Loan Value']
df_loanvalue['Value'] = convert_value_col(df_loanvalue['Value'])

bank_holdings = bank_holdings.merge(df_loanvalue, on='File Date')
bank_holdings = bank_holdings[['File Date', 'Platform', 'Market Value', 'Value']]
bank_holdings.rename(columns={'Value': 'Loan Value'}, inplace=True)
bank_holdings['Loan Value'] = np.where(bank_holdings['Platform'] == 'CMC', 0, bank_holdings['Loan Value'])
bank_holdings['Equity'] = bank_holdings['Market Value'] - bank_holdings['Loan Value']

bank_holdings = bank_holdings.merge(df_loandetails[['File Date', 'Interest Rate']], how='left', on='File Date')
bank_holdings['Interest Rate'] = np.where(bank_holdings['Platform'] == 'CMC', 0, bank_holdings['Interest Rate'])
bank_holdings['Interest Rate'] = convert_percent_col(bank_holdings['Interest Rate'])

# add bank holdings diff columns
bank_holdings['diff_1busday_interestrate'] = bank_holdings.groupby('Platform')['Interest Rate'].diff()

bank_holdings['diff_1busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff()
bank_holdings['diff_5busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff(periods=5)
bank_holdings['diff_21busday_marketvalue'] = bank_holdings.groupby('Platform')['Market Value'].diff(periods=21)
bank_holdings['diff_1busday_marketvalue%'] = bank_holdings['diff_1busday_marketvalue'] / \
    (bank_holdings['Market Value'] - bank_holdings['diff_1busday_marketvalue'])
bank_holdings['diff_5busday_marketvalue%'] = bank_holdings['diff_5busday_marketvalue'] / \
    (bank_holdings['Market Value'] - bank_holdings['diff_5busday_marketvalue'])
bank_holdings['diff_21busday_marketvalue%'] = bank_holdings['diff_21busday_marketvalue'] / \
    (bank_holdings['Market Value'] - bank_holdings['diff_21busday_marketvalue'])

bank_holdings['diff_1busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff()
bank_holdings['diff_5busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff(periods=5)
bank_holdings['diff_21busday_equity'] = bank_holdings.groupby('Platform')['Equity'].diff(periods=21)
bank_holdings['diff_1busday_equity%'] = bank_holdings['diff_1busday_equity'] / \
    (bank_holdings['Equity'] - bank_holdings['diff_1busday_equity'])
bank_holdings['diff_5busday_equity%'] = bank_holdings['diff_5busday_equity'] / \
    (bank_holdings['Equity'] - bank_holdings['diff_5busday_equity'])
bank_holdings['diff_21busday_equity%'] = bank_holdings['diff_21busday_equity'] / \
    (bank_holdings['Equity'] - bank_holdings['diff_21busday_equity'])

# create total holdings
total_holdings = bank_holdings.groupby('File Date')[['File Date', 'Market Value', 'Loan Value', 'Equity']]\
    .sum().reset_index()

# add total holdings diff columns
total_holdings['diff_loanvalue'] = total_holdings['Loan Value'].diff()

total_holdings['diff_1busday_marketvalue'] = total_holdings['Market Value'].diff()
total_holdings['diff_5busday_marketvalue'] = total_holdings['Market Value'].diff(periods=5)
total_holdings['diff_21busday_marketvalue'] = total_holdings['Market Value'].diff(periods=21)
total_holdings['diff_1busday_marketvalue%'] = total_holdings['diff_1busday_marketvalue'] / \
    (total_holdings['Market Value'] - total_holdings['diff_1busday_marketvalue'])
total_holdings['diff_5busday_marketvalue%'] = total_holdings['diff_5busday_marketvalue'] / \
    (total_holdings['Market Value'] - total_holdings['diff_5busday_marketvalue'])
total_holdings['diff_21busday_marketvalue%'] = total_holdings['diff_21busday_marketvalue'] / \
    (total_holdings['Market Value'] - total_holdings['diff_21busday_marketvalue'])

total_holdings['diff_1busday_equity'] = total_holdings['Equity'].diff()
total_holdings['diff_5busday_equity'] = total_holdings['Equity'].diff(periods=5)
total_holdings['diff_21busday_equity'] = total_holdings['Equity'].diff(periods=21)
total_holdings['diff_1busday_equity%'] = total_holdings['diff_1busday_equity'] / \
    (total_holdings['Equity'] - total_holdings['diff_1busday_equity'])
total_holdings['diff_5busday_equity%'] = total_holdings['diff_5busday_equity'] / \
    (total_holdings['Equity'] - total_holdings['diff_5busday_equity'])
total_holdings['diff_21busday_equity%'] = total_holdings['diff_21busday_equity'] / \
    (total_holdings['Equity'] - total_holdings['diff_21busday_equity'])

# create email message body
data_updated = min(df_marketdata['File Date'].sort_values(ascending=False).iloc[0],
    df_loandetails['File Date'].sort_values(ascending=False).iloc[0],
    df_summarydata['File Date'].sort_values(ascending=False).iloc[0])

todays_date = datetime.today().strftime('%Y-%m-%d')
yesterdays_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

if data_updated == todays_date:
    data_message = f'Data up to date for today: {data_updated}'
else:
    data_message = f'ERROR - data out of date, most recent date: {data_updated}, todays date: {todays_date}'

# create individual nab messages
nab_equity = bank_holdings[bank_holdings['Platform'] == 'NAB']['Equity'].values[-1]
nab_equity_1daydiff = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_1busday_equity'].values[-1]
nab_equity_1daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_1busday_equity%'].values[-1]
nab_equity_5daydiff = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_5busday_equity'].values[-1]
nab_equity_5daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_5busday_equity%'].values[-1]
nab_equity_21daydiff = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_21busday_equity'].values[-1]
nab_equity_21daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_21busday_equity%'].values[-1]

df_ann_eq_growth = bank_holdings[(bank_holdings['File Date'].isin(['2022-09-12', todays_date])) & \
                                 (bank_holdings['Platform'] == 'NAB')]
nab_eq_growth = round(df_ann_eq_growth['Equity'].diff().values[1], 2)
df_ann_eq_growth['File Date'] = pd.to_datetime(df_ann_eq_growth['File Date'])
nab_eq_percent_ofyear = (df_ann_eq_growth['File Date'].iloc[1] - df_ann_eq_growth['File Date'].iloc[0]).days / 365
annualised_return_percent = round((nab_eq_growth / nab_eq_percent_ofyear)*100 / (nab_equity - nab_eq_growth), 2)

loandetails_daily_diff_rate = bank_holdings[bank_holdings['Platform'] == 'NAB']['diff_1busday_interestrate'].values[-1]

if loandetails_daily_diff_rate != 0:
    ir_message = f"Interest rate has increased by:  {loandetails_daily_diff_rate}%"
else:
    ir_message = ""

summarydata_loan_daily_diff = total_holdings['diff_loanvalue'].values[-1]

if summarydata_loan_daily_diff != 0:
    loan_message = f"Loan amount reduced by:  {as_currency(summarydata_loan_daily_diff*-1)}"
else:
    loan_message = ""

df_units_message = unit_holdings[(unit_holdings['File Date'] == todays_date) & (unit_holdings['diff_1busday_units'] != 0)]
units_message = [units_message_str(x, y, z) for x, y, z in zip(df_units_message['Code'], df_units_message['Units'], \
                                                               df_units_message['diff_1busday_units'])]

units_message_string = """
{}
""".format("\n".join(units_message[0:]))

# create individual cmc messages
cmc_equity = bank_holdings[bank_holdings['Platform'] == 'CMC']['Equity'].values[-1]
cmc_equity_1daydiff = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_1busday_equity'].values[-1]
cmc_equity_1daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_1busday_equity%'].values[-1]
cmc_equity_5daydiff = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_5busday_equity'].values[-1]
cmc_equity_5daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_5busday_equity%'].values[-1]
cmc_equity_21daydiff = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_21busday_equity'].values[-1]
cmc_equity_21daydiff_pct = bank_holdings[bank_holdings['Platform'] == 'CMC']['diff_21busday_equity%'].values[-1]

# create individual total equity messages
total_equity = total_holdings['Equity'].values[-1]
total_equity_1daydiff = total_holdings['diff_1busday_equity'].values[-1]
total_equity_1daydiff_pct = total_holdings['diff_1busday_equity%'].values[-1]
total_equity_5daydiff = total_holdings['diff_5busday_equity'].values[-1]
total_equity_5daydiff_pct = total_holdings['diff_5busday_equity%'].values[-1]
total_equity_21daydiff = total_holdings['diff_21busday_equity'].values[-1]
total_equity_21daydiff_pct = total_holdings['diff_21busday_equity%'].values[-1]


message = f'''
{data_message}

NAB Equity:  {as_currency(nab_equity)}
- daily diff:  {as_currency(nab_equity_1daydiff)} ({round(nab_equity_1daydiff_pct*100, 2)}%)
- 5 bus day diff:  {as_currency(nab_equity_5daydiff)} ({round(nab_equity_5daydiff_pct*100, 2)}%)
- 21 bus day diff:  {as_currency(nab_equity_21daydiff)} ({round(nab_equity_21daydiff_pct*100, 2)}%)
Annualised equity growth since inception 2022-09-12:  {annualised_return_percent}%
{ir_message}
{loan_message}
{units_message_string}

CMC Equity:  {as_currency(cmc_equity)}
- daily diff:  {as_currency(cmc_equity_1daydiff)} ({round(cmc_equity_1daydiff_pct*100, 2)}%)
- 5 bus day diff:  {as_currency(cmc_equity_5daydiff)} ({round(cmc_equity_5daydiff_pct*100, 2)}%)
- 21 bus day diff:  {as_currency(cmc_equity_21daydiff)} ({round(cmc_equity_21daydiff_pct*100, 2)}%)


Total Equity:  {as_currency(total_equity)}
- daily diff:  {as_currency(total_equity_1daydiff)} ({round(total_equity_1daydiff_pct*100, 2)}%)
- 5 bus day diff:  {as_currency(total_equity_5daydiff)} ({round(total_equity_5daydiff_pct*100, 2)}%)
- 21 bus day diff:  {as_currency(total_equity_21daydiff)} ({round(total_equity_21daydiff_pct*100, 2)}%)

'''

message = message.replace("\n\n\n\n\n", "\n\n").replace("\n\n\n\n", "\n\n").replace("\n\n\n", "\n\n")
