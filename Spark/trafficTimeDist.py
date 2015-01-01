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

	def ifWorkHour(aLine):
		arr = aLine.split(',')
		return ( (arr[1] != "Sat") and (arr[1] != "Sun") and ( int(arr[5]) in weekday_workHour ) )

	def ifOffWorkHour(aLine):
		arr = aLine.split(',')
		return ( (arr[1] != "Sat") and (arr[1] != "Sun") and ( int(arr[5]) in weekday_offWorkHour ) )

	#print trip_data.filter(ifWorkHour).count()
	#print trip_data.filter(ifOffWorkHour).count()
	#print trip_data.count()

	weekday_hour_dist =  trip_data.filter(lambda line: ( line.split(',')[1] != "Sat" and line.split(',')[1] != "Sun" ) ).map(lambda line: (int(line.split(',')[5]), 1) ).reduceByKey( lambda a, b: a + b ).collect()
	weekend_hour_dist =  trip_data.filter(lambda line: ( line.split(',')[1] == "Sat" or line.split(',')[1] == "Sun" ) ).map(lambda line: (int(line.split(',')[5]), 1) ).reduceByKey( lambda a, b: a + b ).collect()

	f_out = open(out_filename, 'w')
	f_out.write("weekday:\n")
	for i in weekday_hour_dist:
		f_out.write( str(i[0]) + "\t" + str(i[1]) + "\n" )

	f_out.write("\n\n\nweekend:\n")
	for i in weekend_hour_dist:
		f_out.write( str(i[0]) + "\t" + str(i[1]) + "\n" )

	f_out.close()

if __name__ == "__main__":
	main(sys.argv[1:])