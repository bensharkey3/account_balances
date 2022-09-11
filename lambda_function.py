from headless_chrome import create_driver
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


URL_NAB = os.environ['URL_NAB']
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
    table_current_data = driver.find_element(by=By.ID, value='current-data')
    current_data_html = table_current_data.get_attribute('outerHTML')
    
    
    
    driver.quit()



    
    print(market_data_html)
    print("***************")
    print(current_data_html)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
