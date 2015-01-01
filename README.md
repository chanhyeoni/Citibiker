Citibiker
=========


The data analytics codes that analyze the usage behavior and pattern of the Citibike sharing system of NYC

MapReduce
	removeQuo.java: for ETL, such as removing the quotation mark in the original data
	timeWindow.java: divide data into 1-hour time frame for each station and count the arrival and departure events at each station in that time frame
	tripDuration_userType.java: this computes the trip duration distribution based on different user types (Subscriber of Customer)
	tripNumMonth.java: this computes the number of trips for different month
	tripWeekday.java: for ETL, transform the date format into desired one
	stationOpenTime(1,2,3).java: used to discover when a station is opened and closed
	Preprocess_MapReduce.java : used to preprocess the data such that the time segment would be divided into day/hour/min —> the preprocess file was put in HDFS and used for queries in Hive

Hive 
	timeWindowMonth.q: this further separates the time-framed data into different months, important for clustering
	weekdayMonth.q: this separates the data produced by "tripWeekday.java" into different months
	frequencyCount.q : a list of queries to make frequency counts grouped by the attributes in the history data table

Spark
	timeClustering.py: it composes the time-framed data to form features for time period clustering. The feature is composed of all the stations within the same time frame with their # of arrival and departure events.
	hotMap.py: This helps to find out hot spots (for arrival or departure) during a certain time period and output the result on Google map
	eastVillageDst.py: This helps to find out what are the hot destination for riders travel from east village and outputs the result on Google map
	customer.py: This helps to find out what are the popular places for customers
	trafficTimeDist.py: This compute the traffic distribution for different hours based on weekday and weekend
	Real_Time_Regression.py : This code uses the data about the available bikes and docks and uses the MLLib and GraphLab Create library to create a predictive regression model that is updated every time the new data comes in

log
	station_logger_NYC.py: used to collect the real-time availability data from CitiBike API
	station_logger_DC.py: used to collect the real-time availability data from Divvy API
	station_logger_Chicago.py: used to collect the real-time availability data from Capital BikeShare API
