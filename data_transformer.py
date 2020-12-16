import yfinance as yf
import json
import boto3
kinesis = boto3.client('kinesis', "us-west-2")
def getReferrer():
    tickers = yf.Tickers('FB SHOP BYND NFLX PINS SQ TTD OKTA SNAP DDOG')
    hist = tickers.history(start="2020-12-01", end="2020-12-02", interval = "5m")
    hist=hist.stack()
    hist.reset_index(inplace=True)
    hist['Datetime']= hist['Datetime'].astype(str)
    hist=hist.rename(columns={'High': 'high', 'Low': 'low', 'Datetime': 'ts', 'level_1':'name'})
    hist=hist[['high', 'low', 'ts', 'name']]
    return hist.to_json(orient="records", lines = True)

def lambda_handler(event, context):
    data = getReferrer()
    print(data)
    kinesis.put_record(
            StreamName="STA9760F2020_stream1",
            Data=data,
            PartitionKey="partitionkey")
   
    return{
    'statusCode':200,
        'body':json.dumps("Hello from Lambda!")
    }