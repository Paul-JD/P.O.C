import pandas as pd
from azure.storage.blob import BlobClient
from Utility import Get_dataset_clean_function

blob_client = BlobClient.from_connection_string(
    conn_str=('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
              '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
              '.windows.net'),
    container_name='filestorage',
    blob_name='test.csv')
data = Get_dataset_clean_function.download_data_from_blob('test.csv')

print(data)
file = data.to_csv(encoding='utf-8')
print(file)
blob_client.upload_blob(data)
