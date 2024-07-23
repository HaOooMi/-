import baostock as bs
import pandas as pd
import sqlite3

lg = bs.login()


class StockDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
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
                            isST TEXT

                            )''')
        self.conn.commit()
    #批量插入
    def insert_stock_batch(self, data):
        try:
            self.cur.executemany("INSERT INTO stock_k_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
            self.conn.commit()
            print(f"Inserted stock: {data[1]}")
        except sqlite3.IntegrityError:
            print(f"Stock {data[1]} already exists in the database.")
    def close(self):
        self.conn.close()


#获取日K线数据
def fetch_stock_data(start_date, end_date):
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败，错误码：{lg.error_code}, 错误信息：{lg.error_msg}")
        return []
    #获取沪深300成分股列表
    rs = bs.query_hs300_stocks()
    if rs.error_code != '0':
        print(f"获取股票列表失败，错误码：{rs.error_code}, 错误信息：{rs.error_msg}")
        bs.logout()
        return []

    hs300_stocks_list = []
    while rs.next():
        hs300_stocks_list.append(rs.get_row_data())

    # 定义数据存储列表
    data_list = []
    for stock in hs300_stocks_list:
        code = stock[1]
        print(f"正在获取股票 {code} 的数据")
        rs_k = bs.query_history_k_data_plus(code,
                                            "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,isST",
                                            start_date=start_date, end_date=end_date,
                                            frequency="d", adjustflag="3")
        if rs_k.error_code != '0':
            print(f"获取股票 {code} 的历史K线数据失败，错误码：{rs_k.error_code}, 错误信息：{rs_k.error_msg}")
            continue

        while rs_k.next():
            data_list.append(rs_k.get_row_data())

    bs.logout()

    return data_list



db=StockDB("stock_k_data.db")
start_date = input('请输入开始日期（****-**—**）：')
end_date = input('请输入结束日期（****-**—**）：')
data_list=fetch_stock_data(start_date, end_date)
db.insert_stock_batch(data_list)

