from pyspark import SparkContext

import sys, getopt
import os

def main(argv):

	hour_type = 0 # 0: morning rush hour, 1: evening rush hour
	ArrDep = 1 # 0: arrival 1: departure
	numHot = 25;

	in_filename = ""
	out_filename = ""

	def print_usage():
		print "timeClustering.py [options]"
		print "============================"
		print "options"
		print "============================"
		print "\t-r morning rush hour (0) evening rush hour (1)"
		print "\t-p arrival (0) or departure (1)"
		print "\t-I in_filename"
		print "\t-O out_filename"
		print "\t-n number of hot spots"
		print "\t-H print out this help"
		print "============================"


	try:
		opts, args = getopt.getopt(argv, "Hr:p:n:I:O:")
	except getopt.GetoptError:
		print_usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-H':
			print_usage()
			sys.exit()
		elif opt == '-r':
			hour_type = int(arg)
		elif opt == '-p':
			ArrDep = int(arg)
		elif opt == '-n':
			numHot = int(arg)
		elif opt == '-I':
			in_filename = arg
		elif opt == '-O':
			out_filename = arg

	if ( in_filename == "" or out_filename == "" ):
		print "input and output files need to be specified"
		sys.exit()

	inputFile = "hdfs://babar.es.its.nyu.edu:8020/user/cyc391/" + in_filename  # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	inputData = sc.textFile(inputFile)

	weekday_workHour = [7, 8, 9]
	weekday_offWorkHour = [17, 18, 19]
	print weekday_workHour
	print weekday_offWorkHour

	def loc_workHour(aLine):
		arr = aLine.split(',')
		return arr[5] + "," + arr[6], [int(arr[7]), int(arr[8])] 

	def ifWorkHour(aLine):
		arr = aLine.split(',')
		return ( ( arr[0] != "Sat" and arr[0] != "Sun" ) and ( int(arr[4]) in weekday_workHour ) )

	def ifOffWorkHour(aLine):
		arr = aLine.split(',')
		return ( ( arr[0] != "Sat" and arr[0] != "Sun" ) and ( int(arr[4]) in weekday_offWorkHour ) )

	def makeMap(loc_arr):
		f_first = open('gMap_first', 'r')
		f_second = open('gMap_second', 'r')
		f_map = open(out_filename + ".html", 'w')

		f_map.write( f_first.read() )
		f_map.write( "\n" )

		n = len(loc_arr)
		for i in range(0, n):
			tmp_str = "      ['<h4>" + str(i+1) + "</h4>', " + loc_arr[i][0] + "]"
			if (i < (n-1)):
				tmp_str = tmp_str + ","
			tmp_str = tmp_str + "\n"
			f_map.write(tmp_str)

		f_map.write( f_second.read() )

		f_first.close()
		f_second.close()
		f_map.close()
		#os.system("firefox " + out_filename + ".html")

	workHour_result = []

	if (hour_type == 0): #morning rush hour

		workHour_result = inputData.filter(ifWorkHour).map(loc_workHour).reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]]).collect()

		if (ArrDep == 0): # look at arrival  - departure
			workHour_result.sort(key = lambda loc: (loc[1][0] - loc[1][1]), reverse = True)
		else: # look at departure - arrrival
			workHour_result.sort(key = lambda loc: (loc[1][1] - loc[1][0]), reverse = True)

		makeMap(workHour_result[0:numHot])

	else: #evening rush hour

		workHour_result = inputData.filter(ifOffWorkHour).map(loc_workHour).reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]]).collect()

		if (ArrDep == 0): # look at arrival  - departure
			workHour_result.sort(key = lambda loc: (loc[1][0] - loc[1][1]), reverse = True)
		else: # look at departure - arrrival
			workHour_result.sort(key = lambda loc: (loc[1][1] - loc[1][0]), reverse = True)

		makeMap(workHour_result[0:numHot])

	
	'''
	weighted_lat = 0
	weighted_long = 0
	count_tot = 0
	for i in myDst:
		count_tot = count_tot + float(i[1])
		loc_arr = i[0].split(',')
		weighted_lat = weighted_lat + float( loc_arr[0] ) * float(i[1])
		weighted_long = weighted_long + float( loc_arr[1] ) * float(i[1])

	weighted_dst = []
	weighted_dst.append( (workHour_result[2][0] , 0) )
	weighted_dst.append( ( str(float(weighted_lat)/float(count_tot)) + "," + str(float(weighted_long)/float(count_tot)) , 0 )  )
	makeMap(weighted_dst)
	'''

if __name__ == "__main__":
	main(sys.argv[1:])