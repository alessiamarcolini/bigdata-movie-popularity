import argparse

from hdfs import InsecureClient

parser = argparse.ArgumentParser(description="Upload opusdata csv file to HDFS")
parser.add_argument(
    "opusdata_local_path", type=str, help="Local path of opusdata csv file"
)
args = parser.parse_args()

local_path = args.opusdata_local_path

client_hdfs = InsecureClient("http://localhost:50070", user="hadoop")
uploaded_path = client_hdfs.upload(hdfs_path="/raw/opusdata.csv", local_path=local_path)
