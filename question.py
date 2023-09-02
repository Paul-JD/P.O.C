from io import StringIO

import pandas as pd
from azure.storage.blob import BlobServiceClient

connection_string = ('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
                     '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
                     '.windows.net')
container_name = 'filestorage'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.csv")

# encoding param is necessary for readall() to return str, otherwise it returns bytes
downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
blob_text = downloader.readall()

df = pd.read_csv(StringIO(blob_text), sep=',')
print(df)
