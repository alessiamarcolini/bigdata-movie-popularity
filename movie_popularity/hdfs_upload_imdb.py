from hdfs import InsecureClient
import wget

IMBD_LINKS = {
    'name.basics.tsv.gz': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    'title.akas.tsv.gz': 'https://datasets.imdbws.com/title.akas.tsv.gz',
    'title.basics.tsv.gz': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title.crew.tsv.gz': 'https://datasets.imdbws.com/title.crew.tsv.gz',
    'title.principals.tsv.gz': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'title.ratings.tsv.gz': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

client_hdfs = InsecureClient('http://localhost:50070', user='hadoop')


for name, link in IMBD_LINKS.items():

    # check if file exists
    content = client_hdfs.content(f'/{name}', strict=False)
    

    wget.download(link)

    if not content: # file doesn't exist
        
        uploaded_path = client_hdfs.upload(hdfs_path='/', local_path=name)

    else:
        print('file exists')
        client_hdfs.delete(hdfs_path=f'/tmp/{name}')
        tmp_uploaded_path = client_hdfs.upload(hdfs_path='/tmp', local_path=name)

        checksum_old = client_hdfs.checksum(f'/{name}')['bytes']
        checksum_new = client_hdfs.checksum(tmp_uploaded_path)['bytes']
        

        if checksum_old != checksum_new:
            result = client_hdfs.delete(hdfs_path=f'/{name}')
            if not result:
                print('something strange happened')
            client_hdfs.rename(tmp_uploaded_path, f'/{name}')
            print('file is different: ', checksum_old, checksum_new)


