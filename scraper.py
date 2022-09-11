import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


DRIVER_PATH = r"C:\Users\BenSharkey\.wdm\drivers\chromedriver\win32\100.0.4896.60\chromedriver.exe"
URL_NAB = ""
USERNAME_NAB = ""
PASSWORD_NAB = ""


def initiate_chrome_driver(DRIVER_PATH):
    '''set up the chrome browser driver
    '''
    driver = webdriver.Chrome(ChromeDriverManager().install())
    return driver


def scrape_tables_from_nab_eb(driver, url, username, password):
    '''logs into to NAB eb account and scrapes the position summary table into a df, and the market price data into another df
    '''
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

    # scrape market_data to df
    table_market_data = driver.find_element(by=By.ID, value='market-data')
    df_market_data_raw = pd.read_html(table_market_data.get_attribute('outerHTML'))[0]
    df_market_data = df_market_data_raw[df_market_data_raw['Code'].notnull()]

    # scrape current_data to df
    table_current_data = driver.find_element(by=By.ID, value='current-data')
    df_current_data_raw = pd.read_html(table_current_data.get_attribute('outerHTML'))[0]
    df_current_data = df_current_data_raw
    
    return df_market_data, df_current_data


def main():
    '''runs main function
    '''
    driver = initiate_chrome_driver(DRIVER_PATH)
    df1, df2 = scrape_tables_from_nab_eb(driver, URL_NAB, USERNAME_NAB, PASSWORD_NAB)
    driver.quit()
    
    print(df1)
    print("***************")
    print(df2)


if __name__ == '__main__':
    main()
