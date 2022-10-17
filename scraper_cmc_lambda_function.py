from headless_chrome import create_driver
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
from os import listdir
from os.path import isfile, join
import boto3
import pytz
import datetime


URL_CMC = "https://www.cmcmarketsstockbroking.com.au/"
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']


def lambda_handler(event, context):
    '''runs function
    '''
    driver = create_driver()

    # open website
    driver.get(URL_CMC)

    # enter username
    input_username = driver.find_element(by=By.ID, value='logonAccount')
    input_username.send_keys(USERNAME)

    # enter password
    input_password = driver.find_element(by=By.ID, value='logonPassword')
    input_password.send_keys(PASSWORD)

    # click login
    login_button = driver.find_element(by=By.ID, value='loginButton')
    login_button.click()

    # go to holdings page
    driver.get("https://www.cmcmarketsstockbroking.com.au/Manage/Holding")

    # click download button
    time.sleep(3)
    download_button = driver.find_element_by_css_selector(".inputbutton.long.btn[ng-click^='downloadStockHoldings']")
    download_button.click()
    time.sleep(5)

    # get file from dowload location
    mypath = r"/tmp"
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    files = [k for k in files if 'StockHoldings-' in k]
    file = sorted(files, reverse=True)[0]

    # write csv file to s3
    tz = pytz.timezone('Australia/Melbourne')
    filename = datetime.datetime.now(tz).strftime('%Y-%m-%d--%H-%M-%p') + '.csv'

    with open(mypath + "/" + file, 'rb') as f:
        data = f.read().decode('utf-8')

    s3client = boto3.client('s3')
    s3client.put_object(Body=data, Bucket='account-balances-scraper', Key='cmc-holdings/' + filename)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
