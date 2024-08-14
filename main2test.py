import requests
from bs4 import BeautifulSoup
import csv
import re
import math
import sqlite3
import mysql.connector
import time


csv_columns = ['کد ملک', 'تاریخ اپلود', 'نوع ملک', 'آدرس', 'نوع معامله', 'قیمت واحد', 'قیمت کل', 'طبقه', 'خواب', 'زیربنا', 'تلفن', 'آشپزخانه', 'سرویس', 'کفپوش']

def get_total_pages(types):
    link = f'https://iranfile.ir/properties/{types}?page=1'
    r = requests.get(link)
    soup = BeautifulSoup(r.text, 'html.parser')
    res = soup.find('p', attrs={'id': 'total-rows'}).text.strip()
    pattern = r'\d+'
    match = re.search(pattern, res)
    number = math.ceil(int(match.group()) / 20)
    return number

def get_property_data(types, page):
    link = f'https://iranfile.ir/properties/{types}?page={page}'
    while True:
        try:
            r = requests.get(link)
            break
        except:
            time.sleep(300)
        
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all('tr', attrs={'data-index': re.compile(r'\d+')})
    return rows

def get_additional_info(link2):
    while True:
        try:
            r2 = requests.get(link2)
            break
        except:
            time.sleep(300)

    soup2 = BeautifulSoup(r2.text, 'html.parser')
    building_info = soup2.find('div', class_='row margin-top-15')
    return building_info

def parse_additional_info(building_info):
    data_dict = {
        'طبقه': None, 'زیربنا': None, 'خواب': None, 
        'تلفن': None, 'آشپزخانه': None, 'سرویس': None, 'کفپوش': None
    }
    if building_info:
        tables = building_info.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                key = cells[0].text.strip()
                value = cells[1].text.strip()
                if key in data_dict and data_dict[key] is None:
                    if key == 'طبقه':
                        try:
                            data_dict[key] = int(value)
                        except ValueError:
                            if value == 'همکف':
                                data_dict[key] = 0
                    else:
                        data_dict[key] = value
    return data_dict

def scrape_properties(types, number):
    data_list = []
    visited = set()
    c = 1
    while c <= number:
        if c in visited:
            c += 1
            continue

        rows = get_property_data(types, c)
        for row in rows:
            code = row.find('td', attrs={'data-title': "کد ملک"}).text.strip()
            date = row.find('td', attrs={'data-title': "تاریخ"}).text.strip()
            home_type = row.find('td', attrs={'data-title': "نوع ملک"}).text.strip()
            address = row.find('td', attrs={'data-title': "آدرس"}).text.strip()
            transaction_type = row.find('td', attrs={'data-title': "نوع معامله"}).text.strip()
            price = row.find('td', attrs={'data-title': "قیمت واحد"}).text.strip()
            total_price = row.find('td', attrs={'data-title': "قیمت کل"}).text.strip()
            link2 = row.find('a')['href']

            building_info = get_additional_info(link2)
            additional_info = parse_additional_info(building_info)

            data_dict = {
                'کد ملک': code, 'تاریخ اپلود': date, 'نوع ملک': home_type,
                'آدرس': address, 'نوع معامله': transaction_type,
                'قیمت واحد': price, 'قیمت کل': total_price,
                'طبقه': additional_info['طبقه'], 'زیربنا': additional_info['زیربنا'],
                'خواب': additional_info['خواب'], 'تلفن': additional_info['تلفن'],
                'آشپزخانه': additional_info['آشپزخانه'], 'سرویس': additional_info['سرویس'],
                'کفپوش': additional_info['کفپوش']
            }
            data_list.append(data_dict)
            visited.add(c)

        print(f'Page {c} scraped.')
        time.sleep(11.12 if c % 2 else 21.9)
        c += 1
    return data_list

def save_to_csv(data_list, filename='data2.csv'):
    with open(filename, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
        writer.writeheader()
        for data in data_list:
            writer.writerow(data)

types = 'buy'
number = get_total_pages(types)
data_list = scrape_properties(types, number)
save_to_csv(data_list)
print('Data saved to CSV.')
    

# Save data to CSV
with open('data.csv', 'w', encoding='utf-8', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
    writer.writeheader()
    for data in data_list:
        writer.writerow(data)

# Save data to MySQL
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="iranfiles"    # +s
)

cursor = db_connection.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS data (
    code INTEGER,
    date TEXT,
    type TEXT,
    address TEXT,
    transaction_type TEXT,
    price TEXT,
    total_price TEXT,
    floor INTEGER,
    bedrooms INTEGER,
    area INTEGER,
    phone TEXT,
    kitchen TEXT,
    bathroom TEXT,
    flooring TEXT
)''')
for i in data_list:
    data = list(i.values())
    cursor.execute("INSERT INTO data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
        int(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], int(data[7]), int(data[8]), int(data[9]), data[10], data[11], data[12], data[13]
    ))
db_connection.commit()
cursor.close()
db_connection.close()

# Save data to SQLite
conn = sqlite3.connect('iranfile.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS data (
    code INTEGER,
    date TEXT,
    type TEXT,
    address TEXT,
    transaction_type TEXT,
    price TEXT,
    total_price TEXT,
    floor INTEGER,
    bedrooms INTEGER,
    area INTEGER,
    phone TEXT,
    kitchen TEXT,
    bathroom TEXT,
    flooring TEXT
)''')

for i in data_list:
    data = list(i.values())
    cursor.execute("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
        int(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], int(data[7]), int(data[8]), int(data[9]), data[10], data[11], data[12], data[13]
    ))

conn.commit()
cursor.close()
conn.close()