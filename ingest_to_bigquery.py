
from datetime import datetime
from google.cloud import bigquery

import pandas as pd

import uuid


class GoogleHelper:
    
    def run_query_dataframe(self, query:str):
        print(query)
        client    = bigquery.Client()
        query_job = client.query(query)
        return query_job.to_dataframe()

    def insert_truncate(self, dataframe, table_id):
        # load the data with write truncate
        job_config = bigquery.LoadJobConfig(
            write_disposition     = "WRITE_TRUNCATE",
            create_disposition    = "CREATE_IF_NEEDED",
            ignore_unknown_values = True,
            autodetect            = True,
            time_partitioning     = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY, # type of partitioning by day
                field = "updated_at",                    # Name of the column to use for partitioning
            )
        )
            
        # insert the data to bigquery
        client = bigquery.Client()
        job    = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Insert WRITE_TRUNCATE Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        
        return None
        
class Repository():
    
    def __init__(self):
        self.project_id = "kbi-krom-dtwrhs-l0-stg-a9a9"
        self.google     = GoogleHelper()
    
    def main(self):
        table_name_duplicate      = "product_duplicates"
        table_name_duplicate_list = "product_duplicate_lists"
        dataset                   = "testing"
        query_duplicate           = f"""SELECT tags, title, category, count(*) as total FROM `{self.project_id}.{dataset}.products` group by tags, title, category HAVING COUNT(*) > 1 order by total desc;"""
        records_merge             = self.google.run_query_dataframe(query_duplicate)
        if len(records_merge) < 1:
            return None # return if doesnot have any data
        
        print(datetime.now().timestamp())
        product_duplicates = pd.DataFrame()
        product_duplicates['title']      = records_merge['title']
        product_duplicates['id']         = [str(uuid.uuid4()) for _ in range(len(product_duplicates))]
        product_duplicates['created_at'] = datetime.now()
        product_duplicates['updated_at'] = datetime.now()
        print(product_duplicates) # log data
        
        product_duplicates = product_duplicates.drop_duplicates(subset=['title']) # delete duplicate data by title
        self.google.insert_truncate(product_duplicates, f"{self.project_id}.{dataset}.{table_name_duplicate}")

        list_titles = product_duplicates['title'].str.replace('"', "'").to_numpy()
        titles      = ','.join(f'"{x}"' for x in list_titles)
        query       = f"""SELECT id as product_id, external_id, title FROM `{self.project_id}.{dataset}.products` WHERE title IN ({titles});"""
        records     = self.google.run_query_dataframe(query)
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
        self.google.insert_truncate(product_duplicates, f"{self.project_id}.{dataset}.{table_name_duplicate_list}")

        return None
        
if __name__ == '__main__':
    repository = Repository()
    repository.main() # call main