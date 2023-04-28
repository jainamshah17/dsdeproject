import pandas as pd
import urllib.parse
import awswrangler as wr

s3_cleansed_layer = "s3://dsde-final-project-cleansed/youtube"
glue_catalog_db_name = "dsde-final-project-youtube-cleaned"
write_data_operation = "append"
glue_catalog_table_name = "cleaned_statistics_reference_data"

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding = 'utf-8')

    try:
        # Create pandas dataframe
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Fetch required features / columns
        df_step = pd.json_normalize(df_raw['items'])

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df = df_step,
            path = s3_cleansed_layer,
            dataset = True,
            database = glue_catalog_db_name,
            table = glue_catalog_table_name,
            mode = write_data_operation
        )

        # Return 
        return wr_response

    except Exception as e:
        print(e)
        print(f"Error getting object {key} from bucket {bucket}.")
        raise e
