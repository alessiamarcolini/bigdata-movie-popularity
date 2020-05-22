from hdfs import InsecureClient

client_hdfs = InsecureClient('http://localhost:50070', user='hadoop')

local_path = 'data/opusdata.csv'

uploaded_path = client_hdfs.upload(hdfs_path='/raw/opusdata.csv', local_path=local_path)
