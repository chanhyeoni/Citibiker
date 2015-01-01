import requests
import time
import datetime
import xml.etree.ElementTree as ET

while True:
	r = requests.get('https://www.capitalbikeshare.com/data/stations/bikeStations.xml') #Washington D.C. Capital Bikeshare station API
	root = ET.fromstring(r.text)
	file_name = datetime.date.fromtimestamp(int(root.attrib['lastUpdate'])/1000).strftime('%Y-%m-%d')
	f = open(file_name, 'a')
	f.write(r.text)
	f.write('\n')
	f.close()
	time.sleep(60)