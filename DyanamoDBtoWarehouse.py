from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

def handle_insert(record):
    print("Handling Insert: ", record)
    data = {}
    for key, value in record['dynamodb']['NewImage'].items():
        data[key] = list(value.values())[0]
    return pd.DataFrame([data])

def lambda_handler(event, context):
    print("Event Received:", event)
    df = pd.DataFrame()

    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]
        if record['eventName'] == "INSERT":
            dff = handle_insert(record)
            df = pd.concat([df, dff], ignore_index=True)

    if not df.empty:
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{table}_{timestamp}.csv"
        key = f"snowflake/{filename}"//file inside s3 bucket

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3 = boto3.client('s3')
        bucketName = "de-project-datewdata"//S3 bucket name

        try:
            s3.put_object(Bucket=bucketName, Key=key, Body=csv_buffer.getvalue())
            print(f"File uploaded to S3: {key}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

    print('Successfully processed %s records.' % str(len(event['Records'])))
