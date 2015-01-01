import os
import sys, getopt

years = [2013, 2014]
month = ["no", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

for i in range(0, 2):
	if i == 0:
		monRange = range(7, 13)
	else:
		monRange = range(1, 9)

	for j in monRange:
		#os.system("spark-submit --master yarn timeClustering.py -c 4 -i 50 -r 50 -I CitiBike/timeWindowMonth/" + str(years[i]) + "_" + month[j] + "/* -O timeWindowMonth/" + str(years[i]) + "_" + month[j] + ".txt")
		print "spark-submit --master yarn timeClustering.py -c 4 -i 50 -r 50 -I CitiBike/timeWindowMonth/" + str(years[i]) + "_" + month[j] + "/* -O timeWindowMonth/" + str(years[i]) + "_" + month[j] + ".txt"