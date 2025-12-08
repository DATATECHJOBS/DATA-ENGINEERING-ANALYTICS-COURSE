import json
#step1
import os
from pycoingecko import CoinGeckoAPI
import boto3
from datetime import datetime
def lambda_handler(event, context):
    #step2 get environment variable
    usd = os.environ['currency_var']
    #step3 add code
    # Extract Data from CoinGecko
    cg = CoinGeckoAPI() #This creates a CoinGeckoAPI object from the pycoingecko library.

    # Get top 50 cryptocurrencies by market cap in USD
    raw_data = cg.get_coins_markets(vs_currency=usd, per_page=50)

    #print(raw_data[0])
    #raw_data = raw_data[0]
    #next we do
    s3_client = boto3.client('s3')
    
    
    filename = "cryptodata_raw_" + str(datetime.now()) + ".json"
    s3_client.put_object(
        Bucket="bitcoindatapipelineproject",
        Key="raw_layer/unprocessed_raw/" + filename,
        Body=json.dumps(raw_data)
        )



    ####added later
    glue = boto3.client("glue") #We are creating a Glue client using the boto3 library.
    job_name = "Transformation Job" #We are creating a Glue client using the boto3 library.

    try:
        # Start the Glue job
        response = glue.start_job_run(JobName=job_name) #AWS Glue gives us back a response that includes a unique ID for this specific run.
        run_id = response['JobRunId'] #We store that unique run ID so we can check on this job later.

        # Check the job status
        status = glue.get_job_run(JobName=job_name, RunId=run_id)
        print("Job Status:", status['JobRun']['JobRunState']) #Possible answers: RUNNING, SUCCEEDED, FAILED, STOPPED.

    except Exception as e:
        print("Error:", e)













    