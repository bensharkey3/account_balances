import json
import pandas as pd
import boto3
import os
from io import StringIO
from datetime import datetime, timedelta


def convert_value_col(df_col):
    '''change data type from dollars in string to float
    '''
    df_col = df_col.replace({'\$':''}, regex = True)
    df_col = df_col.replace({'\,':''}, regex = True)
    df_col = df_col.astype('float')
    return df_col


def as_currency(amount):
    if amount >= 0:
        return "${:,.0f}".format(amount)
    else:
        return "-${:,.0f}".format(-amount)


def units_message_str(code, units, units_change):
    units_message = f"Units added {code}:  {units} (+{units_change})"
    return units_message

    
def lambda_handler(event, context):

    # os.environ["AWS_ACCESS_KEY_ID"] = ''
    # os.environ["AWS_SECRET_ACCESS_KEY"] = ''

    # AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    # AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3client = boto3.client('s3')

    # s3client = boto3.client(
    #     "s3",
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    #       )

    bucket_name = 'account-balances-scraper'

    # get all object keys in bucket
    s3resource = boto3.resource('s3')
    bucket = s3resource.Bucket(bucket_name)

    # get SNS arn
    SNS_ARN = os.environ['SNS_ARN']


    # create marketdata df
    df_marketdata = pd.DataFrame(columns=['Code', 'Security Name', 'Units', 'Price', 'Market Value',
                            'Security Ratio', 'Security Value', 'File Date'])

    for obj in bucket.objects.filter(Prefix='market-data-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]
        
        # clean df
        df_file = df_file[:-1]
        df_file.drop('Guarantor', axis=1, inplace=True)

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_marketdata = pd.concat([df_marketdata, df_file], axis=0)
        df_marketdata.reset_index(drop=True, inplace=True)

        
    # create interest df
    df_interest = pd.DataFrame(columns=['Interest Charged', 'Interest Earned'])

    for obj in bucket.objects.filter(Prefix='interest-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_interest = pd.concat([df_interest, df_file], axis=0)
        df_interest.reset_index(drop=True, inplace=True)


    # create loandetails df
    df_loandetails = pd.DataFrame(columns=['Account Repayment Style', 'Type', 
                                        'Interest Rate', '# of Repayments Remaining', 
                                        'Estimated Expiry Date'])

    for obj in bucket.objects.filter(Prefix='loan-details-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_loandetails = pd.concat([df_loandetails, df_file], axis=0)
        df_loandetails.reset_index(drop=True, inplace=True)


    # create summarydata df
    df_summarydata = pd.DataFrame(columns=[0, 1, 'File Date'])

    for obj in bucket.objects.filter(Prefix='summary-data-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_summarydata = pd.concat([df_summarydata, df_file], axis=0)
        df_summarydata.reset_index(drop=True, inplace=True)


    # create transactions df
    df_transactions = pd.DataFrame(columns=['Date', 'Transaction Details', 'Debit', 'Credit', 'Balance', 'File Date'])

    for obj in bucket.objects.filter(Prefix='transactions-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_transactions = pd.concat([df_transactions, df_file], axis=0)
        df_transactions.reset_index(drop=True, inplace=True)


    # final cleaning before writing to s3
    # rename columns
    df_summarydata.rename(columns={0: 'Measure', 1: 'Value'}, inplace=True)

    # convert column types
    df_transactions.replace(to_replace="No transaction data found for this facility ID.", value=None, inplace=True)

    df_marketdata['Market Value'] = convert_value_col(df_marketdata['Market Value'])
    df_marketdata['Price'] = convert_value_col(df_marketdata['Price'])
    df_marketdata['Security Value'] = convert_value_col(df_marketdata['Security Value'])
    df_interest['Interest Charged'] = convert_value_col(df_interest['Interest Charged'])
    df_interest['Interest Earned'] = convert_value_col(df_interest['Interest Earned'])
    df_transactions['Credit'] = convert_value_col(df_transactions['Credit'])
    df_transactions['Balance'] = convert_value_col(df_transactions['Balance'])

    df_summarydata['File Date'] = pd.to_datetime(df_summarydata['File Date'])
    df_marketdata['File Date'] = pd.to_datetime(df_marketdata['File Date'])
    df_interest['File Date'] = pd.to_datetime(df_interest['File Date'])
    df_summarydata['File Date'] = pd.to_datetime(df_summarydata['File Date'])
    df_loandetails['File Date'] = pd.to_datetime(df_loandetails['File Date'])
    df_transactions['File Date'] = pd.to_datetime(df_transactions['File Date'])


    # put transformed df's into a new bucket location
    csv_buffer_marketdata = StringIO()
    df_marketdata.to_csv(csv_buffer_marketdata)
    s3resource.Object(bucket_name, 'transformed/' + 'marketdata.csv').put(Body=csv_buffer_marketdata.getvalue())

    csv_buffer_interest = StringIO()
    df_interest.to_csv(csv_buffer_interest)
    s3resource.Object(bucket_name, 'transformed/' + 'interest.csv').put(Body=csv_buffer_interest.getvalue())

    csv_buffer_loandetails = StringIO()
    df_loandetails.to_csv(csv_buffer_loandetails)
    s3resource.Object(bucket_name, 'transformed/' + 'loandetails.csv').put(Body=csv_buffer_loandetails.getvalue())

    csv_buffer_summarydata = StringIO()
    df_summarydata.to_csv(csv_buffer_summarydata)
    s3resource.Object(bucket_name, 'transformed/' + 'summarydata.csv').put(Body=csv_buffer_summarydata.getvalue())

    csv_buffer_transactions = StringIO()
    df_transactions.to_csv(csv_buffer_transactions)
    s3resource.Object(bucket_name, 'transformed/' + 'transactions.csv').put(Body=csv_buffer_transactions.getvalue())

    # create email message body
    data_updated = min(df_marketdata['File Date'].sort_values(ascending=False).iloc[0],
        df_interest['File Date'].sort_values(ascending=False).iloc[0],
        df_loandetails['File Date'].sort_values(ascending=False).iloc[0],
        df_transactions['File Date'].sort_values(ascending=False).iloc[0],
        df_summarydata['File Date'].sort_values(ascending=False).iloc[0]).strftime('%Y-%m-%d')

    todays_date = datetime.today().strftime('%Y-%m-%d')
    yesterdays_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    if data_updated == todays_date:
        data_message = f'Data up to date for today: {data_updated}'
    else:
        data_message = f'ERROR - data out of date, most recent date: {data_updated}, todays date: {todays_date}'

    summarydata_temp = df_summarydata[df_summarydata['Measure'] == 'Net Equity'].sort_values(by='File Date', ascending=False).head(5)
    summarydata_temp['Value'] = convert_value_col(summarydata_temp['Value'])
    summarydata_temp['diff'] = summarydata_temp['Value'].diff(periods=-1)
    summarydata_temp['percent_change'] = summarydata_temp['diff']*100 / summarydata_temp['Value']
    summarydata_daily_diff_amount = round(summarydata_temp['diff'].iloc[0], 2)
    summarydata_daily_diff_percent = round(summarydata_temp['percent_change'].iloc[0], 2)
    summarydata_daily_equity_amount = summarydata_temp['Value'].iloc[0]

    loandetails_temp = df_loandetails.sort_values(by='File Date', ascending=False).head(10)
    loandetails_temp['Interest Rate'] = loandetails_temp['Interest Rate'].str.replace('%', '')
    loandetails_temp['Interest Rate'] = loandetails_temp['Interest Rate'].astype(float)
    loandetails_temp['rate_diff'] = loandetails_temp['Interest Rate'].diff(periods=-1)
    loandetails_daily_diff_rate = loandetails_temp['rate_diff'].iloc[0]

    if loandetails_daily_diff_rate != 0:
        ir_message = f"Interest rate has increased by:  {loandetails_daily_diff_rate}%"
    else:
        ir_message = ""

    summarydata_loan = df_summarydata[df_summarydata['Measure'] == 'Loan Value'].sort_values(by='File Date', ascending=False).head(5)
    summarydata_loan['Value'] = convert_value_col(summarydata_loan['Value'])
    summarydata_loan['Value'] = summarydata_loan['Value'].astype(float)
    summarydata_loan['value_diff'] = summarydata_loan['Value'].diff(periods=-1)
    summarydata_loan_daily_diff = summarydata_loan['value_diff'].iloc[0]

    if summarydata_loan_daily_diff != 0:
        loan_message = f"Loan amount reduced by:  {as_currency(summarydata_loan_daily_diff*-1)}"
    else:
        loan_message = ""

    summarydata_ann_rate = df_summarydata[df_summarydata['Measure'] == 'Net Equity'].sort_values(by='File Date', ascending=False)
    summarydata_ann_rate = summarydata_ann_rate.iloc[[0, -1]]
    summarydata_ann_rate['Value'] = convert_value_col(summarydata_ann_rate['Value'])
    summarydata_ann_rate['value_diff'] = summarydata_ann_rate['Value'].diff(periods=-1)
    summarydata_ann_rate['date_diff'] = summarydata_ann_rate['File Date'].diff(periods=-1)
    value_diff = (summarydata_ann_rate['value_diff'] / summarydata_ann_rate['Value']).iloc[0]
    proportion_of_yr = summarydata_ann_rate['date_diff'].iloc[0].days / 365
    annualised_return = round(value_diff*100 / proportion_of_yr, 1)

    summarydata_temp = df_summarydata[df_summarydata['Measure'] == 'Net Equity'].sort_values(by='File Date', ascending=False)
    summarydata_temp['Value'] = convert_value_col(summarydata_temp['Value'])

    todays_date = summarydata_temp['File Date'].iloc[0]
    days_ago7 = summarydata_temp['File Date'].iloc[0] - pd.to_timedelta(7, unit='d')

    summarydata_temp = summarydata_temp[summarydata_temp['File Date'].isin([days_ago7, todays_date])]
    summarydata_temp['value_diff'] = summarydata_temp['Value'].diff(periods=-1)
    summarydata_temp['diff_percent'] = summarydata_temp['value_diff']*100 / summarydata_temp['Value'].iloc[1]
    summarydata_7day_diff_amount = round(summarydata_temp['value_diff'].iloc[0], 2)
    summarydata_7day_diff_percent = round(summarydata_temp['diff_percent'].iloc[0], 2)

    summarydata_temp = df_summarydata[df_summarydata['Measure'] == 'Net Equity'].sort_values(by='File Date', ascending=False)
    summarydata_temp['Value'] = convert_value_col(summarydata_temp['Value'])
    todays_date = summarydata_temp['File Date'].iloc[0]
    yesterdays_date = todays_date - timedelta(days=1)
    days_ago30 = summarydata_temp['File Date'].iloc[0] - pd.to_timedelta(30, unit='d')
    summarydata_temp = summarydata_temp[summarydata_temp['File Date'].isin([days_ago30, todays_date])]
    summarydata_temp['value_diff'] = summarydata_temp['Value'].diff(periods=-1)
    summarydata_temp['diff_percent'] = summarydata_temp['value_diff']*100 / summarydata_temp['Value'].iloc[1]
    summarydata_30day_diff_amount = round(summarydata_temp['value_diff'].iloc[0], 2)
    summarydata_30day_diff_percent = round(summarydata_temp['diff_percent'].iloc[0], 2)

    marketdata_sorted = df_marketdata[df_marketdata['File Date'].isin([todays_date, yesterdays_date])]
    marketdata_sorted['Units'] = marketdata_sorted['Units'].astype(int)
    marketdata_sorted.sort_values(by=['Security Name', 'File Date'], ascending=False, inplace=True)
    marketdata_sorted['units_yesterday'] = marketdata_sorted.groupby('Security Name')['Units'].shift(-1)   # change to Units (Price)
    marketdata_sorted = marketdata_sorted[marketdata_sorted['File Date'] == todays_date]
    marketdata_sorted['units_change'] = marketdata_sorted['Units'] - marketdata_sorted['units_yesterday']
    marketdata_sorted = marketdata_sorted[marketdata_sorted['units_change'] != 0]

    units_message = [units_message_str(x, y, z) for x, y, z in zip(marketdata_sorted['Code'], marketdata_sorted['Units'], marketdata_sorted['units_change'])]

    units_message_string = """
{}
    """.format("\n".join(units_message[1:]))


    message = f'''
{data_message}

Total Equity:  {as_currency(summarydata_daily_equity_amount)}
- daily diff:  {as_currency(summarydata_daily_diff_amount)} ({summarydata_daily_diff_percent}%)
- 7 day diff:  {as_currency(summarydata_7day_diff_amount)} ({summarydata_7day_diff_percent}%)
- 30 day diff:  {as_currency(summarydata_30day_diff_amount)} ({summarydata_30day_diff_percent}%)

Annualised equity growth since inception: {annualised_return}%
{ir_message}
{loan_message}
    '''

    message += units_message_string

    # send email using SNS
    snsclient = boto3.client('sns')
    response = snsclient.publish(
        TopicArn=SNS_ARN,
        Subject='account-balances',
        Message=message)


    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
