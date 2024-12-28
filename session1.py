import uuid

import happybase 
import pandas as pd

class HbaseExecutor: 
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port
        self.connect = happybase.Connection(self.addr, self.port, 1000)

    def init_executor(self):
        pass
    
    def create_table(self):
        try:
            self.connect.create_table(
                'Orders',
                {
                    'Order Detail': dict(),  # 定义 Order Detail 列族
                    'Transaction': dict()    # 定义 Transaction 列族
                }
            )
        except Exception as e:
            print(f'{e}')
    
    def drop_table(self):
        self.connect.disable_table('Orders')
        self.connect.delete_table("Orders")

    @staticmethod
    def read_from_xlsx(file_path):
        """从题目所给的xlsx里读去数据"""
        df = pd.read_excel(file_path)
        df_aggr = [ item for item in df.head(0).columns]
        col_detail = df.iloc[0].tolist()
        real_data = df.iloc[2:]  # 实际的数据

        real_col, aggr_name_tmp = [], ""
        for index, aggr_name in enumerate(df_aggr):            
            if not aggr_name.startswith('Unnamed'):
                aggr_name_tmp = aggr_name
            real_col.append(aggr_name_tmp + ":" + col_detail[index])

        real_data.columns = real_col
        return real_data 

    def put(self, table_name, data:pd.DataFrame):
        """插入数据"""
        table = self.connect.table(table_name)

        if not table:
            raise ValueError("获取table失败")
        
        col_name = data.columns
        for row in data.values.tolist():
            row_key, insert_data = str(uuid.uuid4()), {}
            for index, item in enumerate(row):
                # 将非字节类型（如 int、float）转换为字节类型
                insert_data[col_name[index]] = str(item).encode('utf-8')
            table.put(row_key, insert_data)
    
    def scan_all(self, table_name):
        """扫描整个表并打印所有行"""
        table = self.connect.table(table_name)
        if not table:
            raise ValueError("获取table失败")

        for key, data in table.scan():
            print(f"Row Key: {key}, Data: {data}")
    
    def delete_all(self, table_name):
        """删除表中的所有数据"""
        table = self.connect.table(table_name)
        if not table:
            raise ValueError("获取table失败")

        # 遍历表中所有的rowkeys并删除数据
        for key, _ in table.scan():
            print(f"Deleting Row Key: {key}")
            table.delete(key)  # 删除每一行的数据 


def main():
    file_path = "./desc/题目1数据/data.xlsx"
    data = HbaseExecutor.read_from_xlsx(file_path)
    print(type(data))
    hbase_executor = HbaseExecutor('localhost', 9090)
    hbase_executor.create_table()

    # hbase_executor.delete_all('Orders')
    hbase_executor.put('Orders', data)
    hbase_executor.scan_all('Orders')

    hbase_executor.drop_table()
    hbase_executor.connect.close()


if __name__ == '__main__':
    main()