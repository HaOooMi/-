import sqlite3
from openpyxl import workbook,load_workbook
import requests
import csv
import json
from pprint import pprint
from datetime import datetime
import datetime



class StockDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS stock (
                            股票代码 TEXT PRIMARY KEY,
                            股票名称 TEXT,
                            所属省份 TEXT,
                            股票上市日期 TEXT,
                            公司成立日期 TEXT,
                            董事长 TEXT
                            )''')
        self.conn.commit()

#单个插入
    def insert_stock(self, org_id,org_name_cn,provincial_name,listed_date,established_date,chairman):
        try:
            self.cur.execute("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?)", (org_id,org_name_cn,provincial_name,listed_date,established_date,chairman))
            self.conn.commit()
            print(f"Inserted stock: {org_id}")
        except sqlite3.IntegrityError:
            print(f"Stock {org_id} already exists in the database.")
    def close(self):
        self.conn.close()

#批量插入
    def insert_stock_batch(self, stock_datas):
        try:
            self.cur.executemany("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?)", stock_datas)
            self.conn.commit()
            print(f"Inserted stock: {stock_datas[0]}")
        except sqlite3.IntegrityError:
            print(f"Stock {stock_datas[0]} already exists in the database.")
    def close(self):
        self.conn.close()

#数据更新（暂未启用）
    # def update_stock(self, org_id,org_name_cn,provincial_name,listed_date,established_date,chairman):
    #     try:
    #         self.cursor.execute('''
    #                UPDATE stock
    #                SET price = ?,
    #                    volume = ?
    #                WHERE org_id = ?
    #            ''', (price, volume, org_id))
    #         self.conn.commit()
    #         print(f"Updated stock: {org_id}")
    #     except sqlite3.Error as e:
    #         print(f"Error updating stock {org_id}: {e}")

#数据删除
    def delete_stock(self, org_id):
        try:
            self.cursor.execute('''
                DELETE FROM stock
                WHERE org_id = ?
            ''', (org_id,))
            self.conn.commit()
            print(f"Deleted stock: {org_id}")
        except sqlite3.Error as e:
            print(f"Error deleting stock {org_id}: {e}")

#数据库关闭
    def close_connection(self):
        self.conn.close()
        print("Database connection closed.")


class StockInfo:
    def __init__(self, db_name):
        self.stock_db = StockDB(db_name)

    def query_by_province(self, province):
        self.stock_db.cur.execute("SELECT * FROM stock WHERE 所属省份=?", (province,))
        rows = self.stock_db.cur.fetchall()
        return rows

    def query_by_listed_date(self, listed_date):
        self.stock_db.cur.execute("SELECT * FROM stock WHERE 股票上市日期=?", (listed_date,))
        rows = self.stock_db.cur.fetchall()
        return rows

    def query_by_established_date(self, established_date):
        self.stock_db.cur.execute("SELECT * FROM stock WHERE 公司成立日期=?", (established_date,))
        rows = self.stock_db.cur.fetchall()
        return rows

    def close_db(self):
        self.stock_db.close()

#定义数据库
db = StockDB("my_stock_database.db")
#数据爬取
headers={
    'Cookie':'xq_a_token=64274d77bec17c39d7ef1934b7a3588572463436; xqat=64274d77bec17c39d7ef1934b7a3588572463436; xq_r_token=3f3592acdffbaaee3a7ea110c5d151d2710b7318; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcyMjczMjcyOCwiY3RtIjoxNzIwNjAwNjgxNDgzLCJjaWQiOiJkOWQwbjRBWnVwIn0.CgBcJzIRu-rCT42pzXdNZFxklSm0EQFMmBRatgG6u81eblOinKoDGEtGcFdhKayGxl01qIeTGD95gpFwm4uRAJNOf8_7J0lDFCjtjp8H10UiUrKrQhR0n9OB-LwMRLxsmjWSNhcw-3t99S8G6wl1R_7qH0Q3GOkHGZc3q_-mWCIO2-DkiJ33ewUkKlI-zhBsaiPEoLclKcMjpl9Lj4upU9A4shMaZmXv8h7GxjapcxzQKMt2qOWI0eWTV-5DEFoT1ON17gtV8xCq3eDZnGGmLz2NQLnwux9F6rU1SY1xUcNaR-sohfyWRVLURtovmJUbBpG86gYfF3TXKV1pscSsVw; cookiesu=651720600685314; u=651720600685314; device_id=0150dfff3adb971683d564ddc4bb39b7'
    ,'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'
        }
for page in range(1,168):
    print(f'======正在采集第{page}页数据内容======')
    url1=f'https://stock.xueqiu.com/v5/stock/screener/quote/list.json?page={page}&size=30&order=desc&order_by=percent&market=CN&type=sh_sz'
    response1 = requests.get(url=url1,headers=headers)
    json_data1=response1.json()
    for index1 in json_data1['data']['list']:
        ch=index1['symbol']
        url2 = f'https://stock.xueqiu.com/v5/stock/f10/cn/company.json?symbol={ch}'
        response2 = requests.get(url=url2, headers=headers)
        json_data2 = response2.json()
        timestamp_established_date = json_data2['data']['company']['established_date'] / 1000
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        established_date = epoch + datetime.timedelta(seconds=timestamp_established_date)
        formatted_established_date = established_date.strftime("%Y-%m-%d")
        timestamp_listed_date = json_data2['data']['company']['listed_date'] / 1000
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        listed_date = epoch + datetime.timedelta(seconds=timestamp_listed_date)
        formatted_listed_date = listed_date.strftime("%Y-%m-%d")
        org_id=index1['symbol']
        org_name_cn=index1['name']
        provincial_name=json_data2['data']['company']['provincial_name']
        chairman=json_data2['data']['company']['chairman']
        date=[(org_id,org_name_cn,provincial_name,formatted_listed_date,formatted_established_date,chairman)]
        db.insert_stock_batch(date)
db.close_connection()