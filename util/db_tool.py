import json
import os
from datetime import datetime
import pandas as pd
import pymysql
from config.get_config_path import get_config_path



def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class DbTools:
    def __init__(self):
        self.db_info = self.load_db_info()
        self.conn = self.connect(self.db_info)

    def load_db_info(self):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            db_info_json = json.load(f)
        return db_info_json

    def connect(self, db_info):
        try:
            conn = pymysql.connect(**db_info)
            print("Connected to amazon_mysql database!")
            return conn
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def connect_close(self):
        try:
            self.conn.close()
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def create_stock_info(self, stock_code, open_price, close_price, high_price, low_price, volume,
                          turnover, amplitude, price_change_rate, price_change_amount, turnover_rate, date):
        conn = self.conn
        cursor = conn.cursor()

        # 检查是否存在完全相同的记录
        check_query = "SELECT COUNT(*) FROM stock_data WHERE stock_code = %s AND date = %s"
        cursor.execute(check_query, (stock_code, date))
        result = cursor.fetchone()

        if result[0] > 0:
            print("Record already exists, skipping insertion.")
        else:
            # 插入新记录
            query = "INSERT INTO stock_data (stock_code, open_price, close_price, high_price, low_price, volume, turnover, amplitude, price_change_rate, price_change_amount, turnover_rate, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
                stock_code, open_price, close_price, high_price, low_price, volume,
                turnover, amplitude, price_change_rate, price_change_amount, turnover_rate, date)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into create_stock_info table")
        self.connect_close()


    async def batch_stock_info(self, updates):
        try:
            async with self.conn.cursor() as cursor:
                for update in updates:
                    # 查询是否已经存在相同的记录
                    query_check = """
                    SELECT COUNT(*) FROM stock_data
                    WHERE `stock_code` = %s AND `date` = %s
                    """
                    await cursor.execute(query_check, (update['stock_code'], update['date']))
                    result = await cursor.fetchone()

                    # 如果不存在，则执行插入操作
                    if result[0] == 0:
                        query_insert = """
                        INSERT INTO expanded_asin_info (`market`, `classification_id`, `Asin`, `Rank`, `Date`)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        await cursor.execute(query_insert, (
                        update['market'], update['classification_id'], update['Asin'], update['Rank'], update['Date']))
                await self.conn.commit()
                print("Records inserted successfully into expanded_asin_info table")
        except Exception as e:
            print(f"Error occurred when inserting into expanded_asin_info: {e}")
        finally:
            # 确保连接关闭
            await self.close_connection()