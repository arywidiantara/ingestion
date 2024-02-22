
from sqlalchemy import create_engine
from datetime import datetime

import pandas as pd

import uuid


class Repository():
    
    def __init__(self):
        creds_db  = {
            "host"    : "127.0.0.1",
            "username": "root",
            "database": "cobain",
            "password": "root1234",
            "port"    : 3306
        }
        self.db = create_engine(f"mysql+pymysql://{creds_db['username']}:{creds_db['password']}@{creds_db['host']}:{creds_db['port']}/{creds_db['database']}")
    
    def main(self):
        table_name_duplicate      = "product_duplicates"
        table_name_duplicate_list = "product_duplicate_lists"
        query_duplicate           = f"""SELECT tags, title, category, count(*) as total FROM products group by tags, title, category HAVING COUNT(*) > 1 order by total desc;"""
        print(query_duplicate)

        records_merge = pd.read_sql(query_duplicate, con = self.db) # get the data from db krom
        if len(records_merge) < 1:
            self.db.execute(f"DELETE FROM {table_name_duplicate};") #  drop all the data on the table based on query
            self.db.execute(f"DELETE FROM {table_name_duplicate_list};") #  drop all the data on the table based on query
            print("Deleted all duplicate data!")
            return None # return if doesnot have any data
        
        print(datetime.now().timestamp())
        product_duplicates = pd.DataFrame()
        product_duplicates['title']      = records_merge['title']
        product_duplicates['id']         = [str(uuid.uuid4()) for _ in range(len(product_duplicates))]
        product_duplicates['created_at'] = datetime.now()
        product_duplicates['updated_at'] = datetime.now()
        print(product_duplicates) # log data
        
        product_duplicates = product_duplicates.drop_duplicates(subset=['title']) # delete duplicate data by title
        
        self.db.execute(f"DELETE FROM {table_name_duplicate};") #  drop all the data on the table based on query
        product_duplicates.to_sql(name=table_name_duplicate, con=self.db, if_exists='append', index=False, chunksize=10000) # insert data to table
        print(f"Success Insert Data to Table {table_name_duplicate}")

        list_titles = product_duplicates['title'].str.replace('"', "'").to_numpy()
        titles      = ','.join(f'"{x}"' for x in list_titles)
        query       = f"""SELECT id as product_id, external_id, title FROM products WHERE title IN ({titles});"""
        print(query)
        
        records = pd.read_sql(query, con = self.db) # get the data from db krom
        print(records)
        
        df_alias  = product_duplicates.rename(columns={'id': 'product_duplicate_id'}) # rename columns
        merged_df = pd.merge(records, df_alias[['title', 'product_duplicate_id']], on='title', how='inner')
        print(merged_df)
        
        # set all data
        merged_df['id']          = [str(uuid.uuid4()) for _ in range(len(merged_df))]
        merged_df['created_at']  = datetime.now()
        merged_df['updated_at']  = datetime.now()
        merged_df['deleted_at']  = None
        print(merged_df)

        merged_df = merged_df.drop('title', axis=1) # delete column title

        self.db.execute(f"DELETE FROM {table_name_duplicate_list};") #  drop all the data on the table based on query
        merged_df.to_sql(name=table_name_duplicate_list, con=self.db, if_exists='append', index=False, chunksize=10000) # insert data to table
        print(f"Success Insert Data to Table {table_name_duplicate_list}")

        return None
        
if __name__ == '__main__':
    repository = Repository()
    repository.main() # call main