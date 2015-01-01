import requests
import time
import datetime

while True:
	r = requests.get('http://www.citibikenyc.com/stations/json') #NYC citibike station API
	r_json = r.json()
	exeTime = r_json['executionTime']
	file_name =  datetime.datetime.strptime(exeTime, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d')
	stationList = r_json['stationBeanList']
	for key in stationList[0].keys():
		print key
	for station in stationList:
		print station['id'], ",", 
		print station['stationName'], ",", 
		print station['stAddress1'], ",", 
		print station['longitude'], ",", 
		print station['latitude'], ",", 
		print station['totalDocks'], ",", 
		print station['availableBikes'], ",", 
		print station['availableDocks'], ",", 
		print station['statusValue'], ",", 
		print station['statusKey'],",", 
		print station['testStation']
	#for aStation in stationList:
	#	print aStation['stationName']
	#f = open(file_name, 'a')
	#f.write(r.text)
	#f.close()
	time.sleep(60)
