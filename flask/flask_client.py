import requests

url = 'http://192.168.1.16:9990/mapping'

files = open('test_csv_data.csv', 'rb')

upload = {'file':files}

res = requests.post(url, files = upload)

#res = requests.post(url, data={'asdf':'1234'})

print(f'get request response: {res.text}')
