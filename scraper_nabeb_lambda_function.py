from headless_chrome import create_driver
import json
import os
import boto3
import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


URL_NAB = "https://equitylending.nab.com.au/Login.aspx"
USERNAME_NAB = os.environ['USERNAME_NAB']
PASSWORD_NAB = os.environ['PASSWORD_NAB']


def lambda_handler(event, context):
    """Run function"""

    driver = create_driver()

    # open a website
    driver.get(URL_NAB)
    
    # enter username
    input_username = driver.find_element(by=By.ID, value='cBodyContainer_cBodyContainer_ucLoginControl_txtUsername')
    input_username.send_keys(USERNAME_NAB)
    
    # enter password
    input_password = driver.find_element(by=By.ID, value='cBodyContainer_cBodyContainer_ucLoginControl_txtPassword')
    input_password.send_keys(PASSWORD_NAB)
    
    #click login
    login_button = driver.find_element(by=By.ID, value='cBodyContainer_cBodyContainer_ucLoginControl_lkbLogin')
    login_button.click()
    
    # click my facility number
    facility_number = driver.find_element(by=By.ID, value='cBodyContainer_cBodyContainer_ucFacilitySummary_ctl00_lkbFacilityID_0')
    facility_number.click()
    
    # scrape market_data to html string
    table_market_data = driver.find_element(by=By.ID, value='market-data')
    market_data_html = table_market_data.get_attribute('outerHTML')
    
    # scrape current_data to to html string
    table_summary_data = driver.find_element(by=By.ID, value='current-data')
    summary_data_html = table_summary_data.get_attribute('outerHTML')
    
    # click transactions link
    transactions = driver.find_element(by=By.ID, value='cBodyContainer_ctl00_hplFacilityTransactions')
    transactions.click()
    
    # scrape transactions to html string
    table_transactions = driver.find_element(by=By.ID, value='transaction-list')
    transactions_html = table_transactions.get_attribute('outerHTML')
    
    # click loan details link
    loan_details = driver.find_element(by=By.ID, value='cBodyContainer_ctl00_hplFacilityLoanDetails')
    loan_details.click()
    
    # scrape loan details to html string
    loan_details = driver.find_element(by=By.ID, value='loan-control')
    loan_details_html = loan_details.get_attribute('outerHTML')
    
    # click interest link
    interest = driver.find_element(by=By.ID, value='cBodyContainer_ctl00_hplSMSFInterest')
    interest.click()
    
    # scrape interest to html string
    table_interest = driver.find_element(by=By.ID, value='interest')
    interest_html = table_interest.get_attribute('outerHTML')

    
    # write html strings to s3 files
    tz = pytz.timezone('Australia/Melbourne')
    filename = datetime.datetime.now(tz).strftime('%Y-%m-%d--%H-%M-%p') + '.txt'
    market_data_html = market_data_html.encode('utf-8')
    summary_data_html = summary_data_html.encode('utf-8')
    transactions_html = transactions_html.encode('utf-8')
    interest_html = interest_html.encode('utf-8')
    
    client = boto3.client('s3')
    client.put_object(Body=market_data_html, Bucket='account-balances-scraper', Key='market-data-html-raw/' + filename)
    client.put_object(Body=summary_data_html, Bucket='account-balances-scraper', Key='summary-data-html-raw/' + filename)
    client.put_object(Body=transactions_html, Bucket='account-balances-scraper', Key='transactions-html-raw/' + filename)
    client.put_object(Body=interest_html, Bucket='account-balances-scraper', Key='interest-html-raw/' + filename)
    client.put_object(Body=loan_details_html, Bucket='account-balances-scraper', Key='loan-details-html-raw/' + filename)

    
    driver.quit()

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
