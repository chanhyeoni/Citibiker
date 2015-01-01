import time
import datetime

years = [2013, 2014]

print "tripduration,timestamp,latitude,longitude"

with open('stationOpenTimeResult_loc.txt', 'r') as fp:
	for line in fp:
		fields = line.strip().split(')')
		loc = fields[0].strip().split('(')[1].split(',')
		loc[0] = loc[0].strip()
		loc[1] = loc[1].strip()

		for i in range(0, 2):
			if i == 0:
				monRange = range(7, 13)
			else:
				monRange = range(1, 9)

			for j in monRange:
				if j == 2:
					dayRange = range(1, 29)
				elif (j == 4) or (j == 6) or (j == 9) or (j == 11):
					dayRange = range(1, 31)
				else:
					dayRange = range(1, 32)

				for k in dayRange:
					for l in range(0, 24):
						aDateTime = datetime.datetime(years[i], j, k, l)
						print "\"-1\",\"", aDateTime.strftime("%Y-%m-%d %H") + ":00:00\",\"", loc[0], "\",\"", loc[1], "\""
						#print aDateTime.strftime("%a"), "\t",
						#print aDateTime.strftime("%b"), "\t",
						#print aDateTime.strftime("%d"), "\t",
						#print aDateTime.strftime("%Y"), "\t",
						#print aDateTime.strftime("%H")
						