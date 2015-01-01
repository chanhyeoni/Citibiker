from pyspark import SparkContext

import sys, getopt
import os

def main(argv):

	out_filename = ""

	def print_usage():
		print "timeClustering.py [options]"
		print "============================"
		print "options"
		print "============================"
		print "\t-O out_filename"
		print "\t-H print out this help"
		print "============================"

	try:
		opts, args = getopt.getopt(argv, "HO:")
	except getopt.GetoptError:
		print_usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-H':
			print_usage()
			sys.exit()
		elif opt == '-O':
			out_filename = arg

	if ( out_filename == "" ):
		print "output file needs to be specified"
		sys.exit()

	inputFile = "hdfs://babar.es.its.nyu.edu:8020/user/cyc391/CitiBike/noQuoWeekday_C/*" # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	inputData = sc.textFile(inputFile)

	hours = range(10, 21)

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


	hotSpot = inputData.filter(lambda aLine: (aLine.split(',')[20] == "Customer") and ( int(aLine.split(',')[5]) in hours ) ).map(lambda aLine: ( str(aLine.split(',')[13]) + "," + str(aLine.split(',')[14]) , 1)).reduceByKey(lambda a, b: a + b).collect()
	hotSpot.sort(key = lambda aTuple: aTuple[1], reverse = True)
	makeMap(hotSpot[0:10])

if __name__ == "__main__":
	main(sys.argv[1:])

