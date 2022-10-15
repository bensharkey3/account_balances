from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
from os import listdir
from os.path import isfile, join
import pandas as pd


DRIVER_PATH = r"C:\Users\Ben\Documents\chromedriver_win32\chromedriver.exe"
URL_CMC = "https://www.cmcmarketsstockbroking.com.au/"
USERNAME = ""
PASSWORD = ""


def initiate_chrome_driver(DRIVER_PATH):
    '''set up the chrome browser driver
    '''
    driver = webdriver.Chrome(ChromeDriverManager().install())
    return driver


driver = initiate_chrome_driver(DRIVER_PATH)

options = webdriver.ChromeOptions()
prefs = {"browser.downloads.dir": r"/tmp", "download.default_directory": r"/tmp", "directory_upgrade": True}
options.add_experimental_option("prefs", prefs)


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

print("print cwd " + os.getcwd())
mypath = r"C:\Users\Ben\Downloads"
files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
files = [k for k in files if 'StockHoldings-' in k]
file = sorted(files, reverse=True)[0]

df = pd.read_csv(mypath + "\\" + file)

