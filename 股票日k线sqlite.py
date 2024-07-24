import baostock as bs
import pandas as pd
import sqlite3
import datetime

class StockDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        # 创建股票基本信息表
        self.cur.execute('''CREATE TABLE IF NOT EXISTS stock_basic_info (
                            code TEXT,
                            code_name TEXT,
                            ipoDate TEXT,
                            outDate TEXT
                            )''')
        # 创建日K线数据表
        self.cur.execute('''CREATE TABLE IF NOT EXISTS stock_k_data (
                            date TEXT,
                            code TEXT,
                            open REAL,
                            high REAL,
                            low REAL,
                            close REAL,
                            preclose REAL,
                            volume REAL,
                            amount REAL,
                            turn REAL,
                            tradestatus TEXT,
                            pctChg REAL,
                            isST TEXT,
                            PRIMARY KEY (date, code)
                            )''')
        self.conn.commit()

    #插入股票基本信息
    def insert_basic_info(self, data):
        try:
            self.cur.executemany("INSERT INTO stock_basic_info VALUES(?, ?, ?, ?)", data)
            self.conn.commit()
        except sqlite3.IntegrityError:
            print(f"股票 {data[0][0]} 已经存在于数据库中。")

    #插入股票日k线信息
    def insert_k_data(self, data):
        try:
            self.cur.executemany("INSERT INTO stock_k_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
            self.conn.commit()
        except sqlite3.IntegrityError:
            print(f"数据插入失败。")

    # 查询指定日期的所有股票的K线数据
    def query_k_data_by_date(self, query_date):
        query = "SELECT * FROM stock_k_data WHERE date = ?"
        self.cur.execute(query, (query_date,))
        rows = self.cur.fetchall()
        for row in rows:
            print(row)


    # 查询相同日期不同年份的K线数据
    def query_k_data_by_same_date_diff_years(self, month_day):
        query = "SELECT * FROM stock_k_data WHERE strftime('%m-%d', date) = ?"
        self.cur.execute(query, (month_day,))
        rows = self.cur.fetchall()
        for row in rows:
            print(row)

    def close(self):
        self.conn.close()

#获取股票基本信息
def fetch_stock_basic_info():
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败，错误码：{lg.error_code}, 错误信息：{lg.error_msg}")
        return []

    stock_list = []
    rs =bs.query_hs300_stocks()
    while rs.next():
        row_data = rs.get_row_data()
        stock_list.append(row_data[1])

    stock_basic_list = []
    for stock in stock_list:
        rs = bs.query_stock_basic(stock)

        while rs.next():
            row_data = rs.get_row_data()
            stock_basic_list.append((row_data[0], row_data[1], row_data[2], row_data[3]))

    bs.logout()
    return stock_basic_list

#获取股票日k线信息
def fetch_stock_k_data(code, start_date, end_date):
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败，错误码：{lg.error_code}, 错误信息：{lg.error_msg}")
        return []

    rs = bs.query_history_k_data_plus(code,
                                      "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,isST",
                                      start_date=start_date, end_date=end_date,
                                      frequency="d", adjustflag="3")
    if rs.error_code != '0':
        print(f"获取股票 {code} 的历史K线数据失败，错误码：{rs.error_code}, 错误信息：{rs.error_msg}")
        bs.logout()
        return []

    k_data_list = []
    while rs.next():
        k_data_list.append(rs.get_row_data())

    bs.logout()
    return k_data_list
def query_k_data_by_date(db_name, query_date):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # 查询指定日期的所有股票的K线数据
    query = "SELECT * FROM stock_k_data WHERE date = ?"
    cur.execute(query, (query_date,))
    rows = cur.fetchall()

    # 打印结果
    for row in rows:
        print(row)

    conn.close()

# 主程序
if __name__ == "__main__":
    db_name = "stock_k_data.db"
    query_date = input("请输入查询日期（YYYY-MM-DD）：")
    query_k_data_by_date(db_name, query_date)

# 主程序
if __name__ == "__main__":
    db = StockDB("stock_k_data.db")
    stock_basic_info =fetch_stock_basic_info()
    db.insert_basic_info(stock_basic_info)
    start_date = input('请输入开始日期：')
    end_date =input('请输入结束日期：')
    for stock in stock_basic_info:
        code = stock[0]
        print(f"正在获取股票 {code} 的日K线数据")
        k_data = fetch_stock_k_data(code, start_date, end_date)
        db.insert_k_data(k_data)

    db.close()