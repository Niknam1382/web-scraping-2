import requests
from bs4 import BeautifulSoup
import csv
import re
import math
import sqlite3
import mysql.connector
import time

csv_columns = ['کد ملک', 'تاریخ اپلود', 'نوع ملک', 'آدرس', 'نوع معامله', 'قیمت واحد', 'قیمت کل', 'طبقه', 'خواب', 'زیربنا', 'تلفن', 'آشپزخانه', 'سرویس', 'کفپوش']

types = 'buy'
data_index = 0

link = f'https://iranfile.ir/properties/{types}?page=1'
r = requests.get(link)
soup = BeautifulSoup(r.text, 'html.parser')
res = soup.find('p', attrs={'id': 'total-rows'}).text.strip()
pattern = r'\d+'
match = re.search(pattern, res)
number = math.ceil(int(match.group()) / 20)
page_counter = number
print(f'{page_counter} page\'s are available\n')

data_list = []
visited = []
c = 1
while True:
    try:
        while True:
            if c in visited: # next -> c+=1 if bug in 
                c += 1
                continue
            link = f'https://iranfile.ir/properties/{types}?page={c}'
            r = requests.get(link)
            soup = BeautifulSoup(r.text, 'html.parser')

            rows = soup.find_all('tr', attrs={'data-index': re.compile(r'\d+')})
            for row in rows:
                code = row.find('td', attrs={'data-title': "کد ملک"}).text.strip()
                date = row.find('td', attrs={'data-title': "تاریخ"}).text.strip()
                home_type = row.find('td', attrs={'data-title': "نوع ملک"}).text.strip()
                address = row.find('td', attrs={'data-title': "آدرس"}).text.strip()
                transaction_type = row.find('td', attrs={'data-title': "نوع معامله"}).text.strip()
                price = row.find('td', attrs={'data-title': "قیمت واحد"}).text.strip()
                total_price = row.find('td', attrs={'data-title': "قیمت کل"}).text.strip()
                link2 = row.find('a')['href']

                r2 = requests.get(link2)
                soup2 = BeautifulSoup(r2.text, 'html.parser')
                building_info = soup2.find('div', class_='row margin-top-15')
                if building_info:
                    tables = building_info.find_all('table')
                    data_dict = {}
                    assigned_values = {
                        'floor': False,
                        'area': False,
                        'bedrooms': False,
                        'phone': False,
                        'kitchen': False,
                        'bathroom': False,
                        'flooring': False
                    }
                    for table in tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all('td')
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            if key == 'طبقه' and not assigned_values['floor']:
                                try:
                                    floor = int(value)
                                    assigned_values['floor'] = True
                                except:
                                    if value == 'همکف' :
                                        floor = 0
                            elif key == 'زیربنا' and not assigned_values['area']:
                                area = value
                                assigned_values['area'] = True
                            elif key == 'خواب' and not assigned_values['bedrooms']:
                                bedrooms = value
                                assigned_values['bedrooms'] = True
                            elif key == 'تلفن' and not assigned_values['phone']:
                                phone = value
                                assigned_values['phone'] = True
                            elif key == 'آشپزخانه' and not assigned_values['kitchen']:
                                kitchen = value
                                assigned_values['kitchen'] = True
                            elif key == 'سرویس' and not assigned_values['bathroom']:
                                bathroom = value
                                assigned_values['bathroom'] = True
                            elif key == 'کفپوش' and not assigned_values['flooring']:
                                flooring = value
                                assigned_values['flooring'] = True
                                
                    data_dict['کد ملک'] = code
                    data_dict['تاریخ اپلود'] = date
                    data_dict['نوع ملک'] = home_type
                    data_dict['آدرس'] = address
                    data_dict['نوع معامله'] = transaction_type
                    data_dict['قیمت واحد'] = price
                    data_dict['قیمت کل'] = total_price
                    data_dict['طبقه'] = floor
                    data_dict['زیربنا'] = area
                    data_dict['خواب'] = bedrooms
                    data_dict['تلفن'] = phone
                    data_dict['آشپزخانه'] = kitchen
                    data_dict['سرویس'] = bathroom
                    data_dict['کفپوش'] = flooring

                    data_list.append(data_dict)
                    visited.append(c)

            time.sleep(11.12 if c % 2 else 21.9)
            print(link)
            c += 1
    except :
        print(f'\nc:{c}')
        c -= 1
        time.sleep(600)
    if c == number + 1:
        break
    

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