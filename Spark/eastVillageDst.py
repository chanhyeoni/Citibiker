from pyspark import SparkContext

import sys, getopt
import os

def main(argv):

	in_filename = ""
	out_filename = ""

	def print_usage():
		print "timeClustering.py [options]"
		print "============================"
		print "options"
		print "============================"
		print "\t-I in_filename"
		print "\t-O out_filename"
		print "\t-H print out this help"
		print "============================"

	try:
		opts, args = getopt.getopt(argv, "HI:O:")
	except getopt.GetoptError:
		print_usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-H':
			print_usage()
			sys.exit()
		elif opt == '-I':
			in_filename = arg
		elif opt == '-O':
			out_filename = arg

	if ( in_filename == "" or out_filename == "" ):
		print "input and output files need to be specified"
		sys.exit()

	tripFile = "hdfs://babar.es.its.nyu.edu:8020/user/cyc391/" + in_filename  # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	trip_data = sc.textFile(tripFile)

	weekday_workHour = [7, 8, 9]
	weekday_offWorkHour = [17, 18, 19]

	'''
	[0] duration	[1] start_week_day	[2] start_month 	[3] start_month_day           
	[4] start_year	[5] start_hour		[6] stop_week_day	[7] stop_month          	                 
	[8] stop_month_day	[9] stop_year	[10] stop_hour	[11] start_id                     
	[12] start_name	[13] start_lat		[14] start_long	[15] stop_id
	[16] stop_name	[17] stop_lat		[18] sopt_long	[19] bike_id                      
	[20] user_type	[21] birth_year		[22] genger        
	'''

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


	upper = 40.734964
	lower = 40.720004
	right = -73.971741
	left = -73.990366

	def ifMatchLoc(aLine):
		arr = aLine.split(',')
		return ( ( float(arr[13]) <= upper ) and ( float(arr[13]) >= lower ) and ( float(arr[14]) <= right ) and ( float(arr[14]) >= left ) and (arr[1] != "Sat" and arr[1] != "Sun") and (int(arr[5]) in weekday_workHour) )

	def mapArrLoc(aLine):
		arr = aLine.split(',')
		return arr[17] + "," + arr[18], 1

	myDst = trip_data.filter(ifMatchLoc).map(mapArrLoc).reduceByKey(lambda a, b: a + b).collect()
	myDst.sort(key = lambda aTuple: aTuple[1], reverse = True)
	makeMap(myDst[0:10])

if __name__ == "__main__":
	main(sys.argv[1:])