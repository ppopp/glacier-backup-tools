import csv

secret_header = 'Secret Access Key'
access_header = 'Access Key Id'
account_header = 'User Name'

def read(credentials_path):
    reader = csv.reader(open(credentials_path, 'r'))
    header = reader.next()
    values = reader.next()
    return dict(zip(header, values))

