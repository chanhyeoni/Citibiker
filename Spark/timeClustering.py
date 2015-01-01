from pyspark import SparkContext
from pyspark.mllib.clustering import *
from math import sqrt
from numpy import *

import sys, getopt
import os

def main(argv): 

	numCluster = 5
	maxIter = 25
	numRun = 25

	in_filename = ""
	out_filename = ""

	def print_usage():
		print "timeClustering.py [options]"
		print "============================"
		print "options"
		print "============================"
		print "\t-c numCluster"
		print "\t-i maxIteration"
		print "\t-r numRun"
		print "\t-I in_filename"
		print "\t-O out_filename"
		print "\t-H print out this help"
		print "============================"


	try:
		opts, args = getopt.getopt(argv, "Hc:i:r:I:O:")
	except getopt.GetoptError:
		print_usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-H':
			print_usage()
			sys.exit()
		elif opt == '-c':
			numCluster = int(arg)
		elif opt == '-i':
			maxIter = int(arg)
		elif opt == '-r':
			numRun = int(arg)
		elif opt == '-I':
			in_filename = arg
		elif opt == '-O':
			out_filename = arg

	if ( in_filename == "" or out_filename == "" ):
		print "input and output files need to be specified"
		sys.exit()

	def lineLoc(aLine):
		attr = aLine.split(',')
		return "(" + attr[5] + attr[6] + ")"

	def timeWindow_Loc(aLine):
		attr = aLine.split(',')
		return attr[0] + "," + attr[1] + "," + attr[2] + "," + attr[3] + "," + attr[4] , attr[5] + " " + attr[6] + " " + attr[7] + " " + attr[8] + " "

	inputFile = "hdfs://babar.es.its.nyu.edu:8020/user/cyc391/" + in_filename  # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	inputData = sc.textFile(inputFile).cache()
	
	#print inputData.map(lineLoc).distinct().count() , "distinct stations"

	feature_loc = inputData.map(timeWindow_Loc).reduceByKey(lambda a, b: a+b)

	#print feature_loc.take(1)

	def rmLoc(aLine):
		result = ""
		arr = aLine[1].split()
		for i in range(0, len(arr)):
			if ( i % 4 == 2 or i % 4 == 3 ):
				result = result + arr[i] + " "
		return aLine[0], result

	ArrDepData = feature_loc.map(rmLoc)

	#print ArrDepData.take(1)
	#print len(ArrDepData.take(1)[0][1].split()) , " elements"

	#print ArrDepData.count() , " points"

	parsedArrDepData = ArrDepData.map(lambda line: array( [float(x) for x in line[1].split()] ))

	clusters = KMeans.train(parsedArrDepData, numCluster, maxIterations=maxIter, runs=numRun, initializationMode='random')

	#print clusters.centers[0]

	def assignCluster(aLine):
		return aLine[0], clusters.predict( array( [float(x) for x in aLine[1].split()] ) )

	dateCluster = ArrDepData.map(assignCluster)

	#print dateCluster.count()

	#dateCluster.saveAsTextFile("dateClusters")

	def weekDayEndCluster(aLine):
		if ( aLine[0].split(',')[0] == "Sat" or aLine[0].split(',')[0] == "Sun" ) :
			return "Weekend," + aLine[0].split(',')[4] + "," + str(aLine[1]), 1
		else :
			return "Weekday," + aLine[0].split(',')[4] + "," + str(aLine[1]), 1

	def weekDayEnd(aLine):
		arr = aLine[0].split(',')
		return arr[0] + "," + arr[1], arr[2] + "," + str(aLine[1])

	def weekDayEndMax(a, b):
		if ( int(a.split(',')[1]) > int(b.split(',')[1]) ):
			return a
		else :
			return b

	dateClusterResult = dateCluster.map(weekDayEndCluster).reduceByKey(lambda a, b: a + b).map(weekDayEnd).reduceByKey(weekDayEndMax).sortByKey(lambda k: k.split(',')[1])
	#os.system("hadoop fs -rm -r dateClusterResult")
	#dateClusterResult.saveAsTextFile("dateClusterResult")

	#os.system("hadoop fs -get dateClusterResult/part-00000 " + out_filename)

	def setCluster2Key(aLine):
		keyArr = aLine[0].split(',')
		return keyArr[0] + "," + aLine[1].split(',')[0], str(keyArr[1])

	def sortHour(aLine):
		hourArr = aLine[1].split();
		for hr in hourArr:
			hr = int(hr)
		sorted(hourArr)
		hrStr = ""
		for hr in hourArr:
			hrStr = hrStr + " " + str(hr)
		return aLine[0], hrStr

	#os.system("hadoop fs -rm -r sortedClusterHour")
	sortedClusterHour = dateClusterResult.map(setCluster2Key).reduceByKey(lambda a, b: a + " " + b).map(sortHour).collect()#saveAsTextFile("sortedClusterHour")
	#os.system("hadoop fs -cat sortedClusterHour/part-00000")
	
	clusterHourList = []
	for i in sortedClusterHour:
		tmp_list = []
		hourArr = i[1].split()
		for j in hourArr:
			tmp_list.append(int(j))
		aTuple = i[0].split(',')[0] , tmp_list
		clusterHourList.append(aTuple)
		
	print clusterHourList

	f_out = open(out_filename, 'w')
	for i in clusterHourList:
		f_out.write(i[0] + "\t")
		for j in i[1]:
			f_out.write(str(j) + " ")
		f_out.write("\n")
	f_out.close()

	weekday_workHour = []
	weekday_offWorkHour = []
	for i in clusterHourList:
		if (i[0] == "Weekday"):
			if (max(i[1]) < 12 and min(i[1]) > 0):
				for j in i[1]: weekday_workHour.append(j)
			if (min(i[1]) > 12 and max(i[1]) < 23):
				for j in i[1]: weekday_offWorkHour.append(j)


if __name__ == "__main__":
	main(sys.argv[1:])