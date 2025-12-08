import json
#1
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def metadata(data):
    metadata_list = []
    for coin in data:
        coin_id = coin['id']
        coin_symbol = coin['symbol']
        coin_name = coin['name']
        coin_image = coin['image']
        coin_rank = coin['market_cap_rank']
        coin_max_supply = coin['max_supply']
        coin_metadata = {
            'coin_id': coin_id,
            'symbol': coin_symbol,
            'name': coin_name,
            'image_url': coin_image,
            'market_cap_rank': coin_rank,
            'max_supply': coin_max_supply
        }
        metadata_list.append(coin_metadata)  
    return metadata_list

def marketdata(raw_data):
    market_data_list = []

    for coin in raw_data:
        coin_id = coin['id']
        current_price = coin['current_price']
        market_cap = coin['market_cap']
        total_volume = coin['total_volume']
        high_24h = coin['high_24h']
        low_24h = coin['low_24h']
        price_change_24h = coin['price_change_24h']
        price_change_pct_24h = coin['price_change_percentage_24h']
        market_cap_change_24h = coin['market_cap_change_24h']
        market_cap_change_pct_24h = coin['market_cap_change_percentage_24h']

        coin_market_data = {
            'coin_id': coin_id,
            'current_price': current_price,
            'market_cap': market_cap,
            'total_volume': total_volume,
            'high_24h': high_24h,
            'low_24h': low_24h,
            'price_change_24h': price_change_24h,
            'price_change_pct_24h': price_change_pct_24h,
            'market_cap_change_24h': market_cap_change_24h,
            'market_cap_change_pct_24h': market_cap_change_pct_24h
        }

        market_data_list.append(coin_market_data)
    return market_data_list

def lambda_handler(event, context):
    #2
    s3 = boto3.client("s3")
    Bucket = "bitcoindatapipelineproject"
    Key = "raw_layer/unprocessed_raw"
    #3
    #print(s3.list_objects(Bucket=Bucket, Prefix=Key))

    #4
    crypto_data = []
    crypto_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        #print(file['Key'])
        file_name = file['Key']
        #print(file_name)
        if file_name.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_name)
            body = response['Body'].read().decode('utf-8')
            jsonObject = json.loads(body)
            #print(jsonObject)
            crypto_data.append(jsonObject)
            crypto_keys.append(file_name)
            #print(crypto_keys)

    for data in crypto_data:
        #print(data)
        metadata_list  = metadata(data)
        #print(metadata_list)
        

        #print(metadata_list)
        df_metadata = pd.DataFrame(metadata_list)
        #print(df_metadata.head())
        df_metadata = df_metadata.drop_duplicates(subset=['coin_id'])
        #print(df_metadata.head())

        market_data_list = marketdata(data)
        #print(market_data_list)
        df_marketdata = pd.DataFrame(market_data_list)
        #print(df_marketdata.head())
        #print(df_marketdata.to_string(index=False))

        df_marketdata = df_marketdata.drop_duplicates(subset=['coin_id'])
        #print(df_marketdata)


        output_location_metadata = "refined_layer/meta_data/meta_data_transformed_" + str(datetime.now()) + ".csv"
        metadatafile_buffer=StringIO()
        df_metadata.to_csv(metadatafile_buffer, index=False)
        metadata_content = metadatafile_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=output_location_metadata, Body=metadata_content)

        output_location_marketdata = "refined_layer/market_data/market_data_transformed_" + str(datetime.now()) + ".csv"
        marketdatafile_buffer=StringIO()
        df_marketdata.to_csv(marketdatafile_buffer, index=False)
        marketdata_content = marketdatafile_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=output_location_marketdata, Body=marketdata_content)

    s3_resource = boto3.resource('s3')
    for key in crypto_keys:
        source_location = {
            'Bucket': Bucket,
            'Key': key
        }
        #['raw_layer/unprocessed_raw/cryptodata_raw_2025-07-17 00:56:15.257982.json']
        s3_resource.meta.client.copy(source_location, Bucket, 'raw_layer/processed_raw/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()
    