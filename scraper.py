from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import psycopg2
from psycopg2 import sql
import time
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
from datetime import date
import logging
from dotenv import load_dotenv
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import sys
import urllib3
import shutil

# Add these near the top of the file, after imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
load_dotenv()
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run Chrome in headless mode
chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

# Force IPv4
urllib3.util.connection.HAS_IPV6 = False
# Setup WebDriver
service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=chrome_options)
print("chrome driver installed")

domain_names = ['front-end','back-end', 'full-stack', 'mobile', 'software-development', 'devops-sre','data-science', 'cyber-security','ai-ml','qa', 'analytics','sales', 'finance','marketing', 'design', 'hr', 'customer-support', 'management','other']
url = 'https://techscene.ee/?domain=
'

column_names =  ['company_name', 'job_name', 'job_link','domain']

df = pd.DataFrame(columns=column_names)
new_row_dict = {}
jobs = {}

def create_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', '5432')
    )
conn = create_db_connection()

for domain in domain_names:
    try:
        new_url = url + urllib.parse.quote_plus(domain)
        logging.info(f"Scraping domain: {domain} at URL: {new_url}")

        driver.get(new_url)
        logging.info(f"Page loaded for domain: {domain}")
        
        # Wait for specific element to ensure page is loaded
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "homePage-company")))
        
        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')
        companies = soup.find_all(class_="homePage-company")
        
        logging.info(f"Found {len(companies)} companies for domain: {domain}")

        for company in companies:
            company_name = company.find(class_="homePage-companyName").text.strip()
            job_links = company.find_all(class_="homePage-job")

            new_row_dict = {}

            for job_link in job_links:
                job_title = job_link.text.strip()
                job_href = job_link['href']
                
                new_row_dict = { 'company_name': company_name, 'job_name': job_title, 'job_link': job_href, 'domain':domain}
                length = len(df)
                
                df.loc[length] = new_row_dict
                
                
            

    except Exception as e:
        logging.error(f"Error processing domain {domain}: {e}")
        continue

# Make sure to quit the driver in a finally block
try:
    # Your existing DataFrame processing code
    df = df.assign(date_added=pd.to_datetime(date.today()))
    logging.info(f"Total jobs scraped: {len(df)}")
finally:
    logging.info("Closing WebDriver...")
    driver.quit()

# Add a counter
rows_inserted = 0
cur = conn.cursor()
for _, row in df.iterrows():
    insert_query = sql.SQL("INSERT INTO {} (company_name, job_name, job_link, date_added, domain) VALUES (%s, %s, %s, %s, %s)").format(sql.Identifier("techscene_jobs"))
    cur.execute(insert_query, (row['company_name'], row['job_name'], row['job_link'], row['date_added'], row['domain']))
    rows_inserted += 1

conn.commit()
logging.info(f"Successfully inserted {rows_inserted} rows into database")
cur.close()
conn.close()
