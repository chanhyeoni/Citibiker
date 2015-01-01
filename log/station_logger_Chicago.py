import requests
import time
import datetime

while True:
	r = requests.get('http://www.divvybikes.com/stations/json') #Chicago Divvy station API
	r_json = r.json()
	exeTime = r_json['executionTime']
	file_name =  datetime.datetime.strptime(exeTime, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d')
	f = open(file_name, 'a')
	f.write(r.text)
	f.close()
	time.sleep(60)
