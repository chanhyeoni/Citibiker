""" 
    Regression model for station feed data
    This is the source code for predicting the bike and dock availability in real-time
    Using the regression model from Spark and GraphLab Create, this code is able to generate
    the predicted number of bikes and docks that might be available
"""

import os
import sys
import requests
import numpy as np
import pandas as pd
import time
import graphlab as gl

os.environ['SPARK_HOME']="/usr/local/spark-1.1.0"
sys.path.append("/usr/local/spark-1.1.0/python/")
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LassoWithSGD
from pyspark.mllib.regression import RidgeRegressionWithSGD

from sklearn.cross_validation import train_test_split


def getStationFeedData():
    """takes the api parameter and retruens a new data from the station feed json file"""
    r = requests.get("http://www.citibikenyc.com/stations/json")
    jsonfile = r.json()
    execution_time = jsonfile['executionTime']

    data = jsonfile['stationBeanList']
    data_df  = pd.DataFrame(data)
    stationName = list(data_df['stationName'])

    n_bikes = list(data_df['availableBikes'])
    n_docks = list(data_df['availableDocks'])
    
    n_bikes_df = {execution_time:pd.Series(n_bikes, index = stationName)}
    n_docks_df = {execution_time:pd.Series(n_docks, index = stationName)}

    n_bikes_df = pd.DataFrame(n_bikes_df)
    n_docks_df = pd.DataFrame(n_docks_df)
    
    return n_bikes_df, n_docks_df

def giveFirstColNames(data):
    return data.columns[0]

def giveLastColNames(data):
    return data.columns[len(data.columns)-1]
 
def giveNColumns(data):
    return len(data.columns)


def station_feed(n_bikes, n_docks):
    # get the station feed data and create a data frame using the new data ready every minute
    # since it is virutally impossible to take all the data every single minute,
    # it would be a good idea to have 20 recent data samples

    n_bikes_new, n_docks_new = getStationFeedData()

    n_bikes = pd.concat([n_bikes, n_bikes_new], axis = 1)
    n_docks = pd.concat([n_docks, n_docks_new], axis = 1)

    n_cols_bikes = giveNColumns(n_bikes)
    n_cols_docks = giveNColumns(n_docks)
    
    first_col_bikes = n_bikes.columns[0]
    first_col_docks = n_docks.columns[0]
    
    mode_bikes = 'w'
    if(n_cols_bikes>=20): 
        del n_bikes[first_col_bikes]
    mode_docks = 'w'
    if(n_cols_docks>=20): 
        del n_docks[first_col_docks]
    
    n_bikes.to_csv("./data/n_bikes.csv", index = False, header = False, index_label = "Unnamed: 0", mode = mode_bikes)
    n_docks.to_csv("./data/n_docks.csv", index = False, header = False, index_label = "Unnamed: 0", mode = mode_docks)    
    
    return n_bikes, n_docks
    	
def convertToSFrame(data):
    # convert the data frame of any allowable type to grpahlab's SFrame
    data = gl.SFrame(data)
    return data

def splitIntoTrainTestData(data, ratio = 0.4):
    # split the data into training and test data - the cross-validation of ratio 0.4
    x_train, y_train, x_test, y_test = train_test_split(data[0:len(data)-2],data[len(data)-1], test_ratio = ratio)
    train = pd.concat([x_train, y_train], axis = 1)
    test = pd.concat([x_test, y_test], axis = 1)
    return train, test


def regression_graphlab(data):
    # regression method from the graphlab library
    target_name = giveLastColNames(data)
    
    # the cross-validation
    train, test = splitIntoTrainTestData(data)

    # the linear regression line
    linear_model = gl.linear_regression.create(train, target = target_name)
    result_linear = linear_model.evaluate(test)
    rmse_linear = result_linear.value()[0]
    
    # the boosted tree regression line
    boosted_tree_model = gl.boosted_tree_regression.create(train, target = target_name)
    result_boosted_tree = boosted_tree_model.evaluate(test)
    rmse_boosted_tree = result_boosted_tree.value()[0]

	# return the model that has less root mean square error than the other mode
    if(rmse_boosted_tree < rmse_linear):
        return boosted_tree_model
    else:
        return linear_model

def createLabeledPoint(aRecord):
    # aRecord is the string that has the data and the commas between them
    # target should be the most recent one, and the 
    ls_record = [int(x) for x in aRecord.replace(',', ' ').split(' ')]
    target = ls_record[len(ls_record)-1]
    data = ls_record[0:(len(ls_record)-2)]
    return LabeledPoint(target, data)

def convertTOLabeledPoint(sc, filename):
    # the data passed in as a parameter is data frame
    # 
    raw_data = sc.textFile(filename)
    parsed_data = raw_data.map(createLabeledPoint)
    return parsed_data
    
def convertRDDtoSFrame(RDD_data):
    return 0
    
def configureSpark(app_name):
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)
    return sc

def regression_spark(sc, filename):
    # regression method from the spark library. the data passed in is the data frame of pandas
    # must take the SparkContext object to parallelize the data
    # convert to Labled Point
    data = convertTOLabeledPoint(sc, filename)
    
    # again, test every single different model and pick out the one that gives you the lowest error
    model_linear = LinearRegressionWithSGD.train(data)
    model_lasso = LassoWithSGD.train(data)
    model_ridge = RidgeRegressionWithSGD.train(data)
    
    # error comparison
    valuesAndPreds_linear = data.map(lambda p: (p.label, model_linear.predict(p.features)))
    MSE_linear = valuesAndPreds_linear.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds_linear.count()
    
    valuesAndPreds_lasso = data.map(lambda p: (p.label, model_lasso.predict(p.features)))
    MSE_lasso = valuesAndPreds_lasso.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds_lasso.count()
    
    valuesAndPreds_ridge = data.map(lambda p: (p.label, model_ridge.predict(p.features)))
    MSE_ridge = valuesAndPreds_ridge.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds_ridge.count()
    
    # initialize the model to return 
    model = model_linear
    if(MSE_linear < MSE_lasso and MSE_linear < MSE_ridge):
        model = model_linear
    elif(MSE_lasso < MSE_ridge and MSE_lasso < MSE_linear):
        model = model_lasso
    elif(MSE_ridge < MSE_lasso and MSE_ridge < MSE_linear):
        model = model_ridge

    return model


def getTestData(data_df):
    # convert the data into the numpy array data
    data_df_columns = data_df.columns
    data_idx = data_df_columns[0:len(data_df.columns)-1]
    test_data = data_df[data_idx]
    return np.array(test_data)

def main():
    n_bikes, n_docks = getStationFeedData()
    timer = 0
    
    # spark setting
    sc_regression = configureSpark("Regression")
    
    while (1):
        # train the model
        n_bikes, n_docks = station_feed(n_bikes, n_docks)
        if(timer >= 20):
            parsed_data = convertTOLabeledPoint(sc_regression, "./data/n_bikes.csv")
            spark_model = regression_spark(sc_regression, "./data/n_bikes.csv")
            predicted = parsed_data.map(lambda p: (p.label, spark_model.predict(p.features)))
            stations = n_bikes.index
            for i in range(len(stations)):
                print str(stations[i]) + " will have " + str(predicted.values().collect()[i]) + " bikes available"
            
            # graphlab regression
            #bikes_data = convertToSFrame(n_bikes)
            #docks_data = convertToSFrame(n_docks)
            #best_regression_model_bikes = regression_graphlab(bikes_data)
            #best_regression_model_docks = regression_graphlab(docks_data)
    		# predict in real-time how many bikes and docks might be used
            #print best_regression_model_bikes.predict(bikes_data)
            #print best_regression_model_docks.predict(docks_data)
        time.sleep(60)
        timer += 1 
        # can be set differently, since change the number of bikes minute by minute
        # does not reflect the big change in the number of bikes being used.
        print timer 
        

if __name__ == "__main__":
    main()