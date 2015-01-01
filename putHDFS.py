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
		#os.system("hadoop fs -mkdir CitiBike/noQuoWeekdayMonth/" + str(years[i]) + "_" + month[j])
		os.system("hadoop fs -put noQuoWeekday_" + str(years[i]) + "_" + month[j] + "/* CitiBike/noQuoWeekdayMonth/" + str(years[i]) + "_" + month[j])