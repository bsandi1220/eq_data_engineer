#!/usr/bin/env python3

import argparse
import time

from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, StructField, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import sin, cos, atan2, toRadians, pow
import pyspark.sql.functions as f
from pyspark.sql import Window
import math
from pyspark.sql.functions import count



'''
The present script executes in Spark the phases:

1. Cleanup
2. Label
3. Analysis

More information about them is given in the README.md file 
'''



if __name__=='__main__':
    


    spark = SparkSession\
        .builder\
        .appName("LocationPipeline")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')
    
    ### 1. Cleanup
    
    ## Location Schema
    locationSchema = StructType([
    # ID
    StructField('id',IntegerType(), False),

    # TimeSt
    StructField('timeSt',TimestampType(), False),

    # Country
    StructField('country', StringType (), True),

    # Province
    StructField('province', StringType (), True),

    # City
    StructField('city', StringType (), False),

    # Latitude
    StructField('Latitude', FloatType (), False),

    # Longitude
    StructField('Longitude', FloatType (), False),

    ])

    ## Load data 
    location_df = spark.read.csv('/tmp/data/DataSample.csv', header=True, schema= locationSchema)


    ## Drop suspicious request records 
    location_df_clean = location_df.withColumn("sus", count("*").over(Window.partitionBy(location_df['timeSt'],location_df['Latitude'],location_df['Longitude'])))
    location_df_clean = location_df_clean.filter(location_df_clean['sus']==1)
    

    
    
    ### 2. Label

    ## Schema POI

    poiSchema = StructType([
    # POIID
    StructField('POIID', StringType(), False),

    # Latitude
    StructField('Lat', FloatType (), False),

    # Longitude
    StructField('Lon', FloatType (), False),

    ])


    ## Load data 
    poi_df = spark.read.csv('/tmp/data/POIList.csv', header=True, schema= poiSchema)


    ## Function to calculate distance 
    def distance(lat, lon, lat2, lon2):
        '''
        Uses the "haversine" formula to calculate the distance between two points
        using they latitude and longitude

        Parameters
        ----------
        lat: latitude co-ordinate using signed decimal degrees without compass direction for first location 
        lon: longitude co-ordinate using signed decimal degrees without compass direction for first location 
        lat2: latitude co-ordinate using signed decimal degrees without compass direction for second location 
        lon2: longitude co-ordinate using signed decimal degrees without compass direction for second location 

        Returns
        -------
        Returns distance between two points
    
    
        Notes
        -----
        Haversine formula
        Δφ = φ1 - φ2
        Δλ = λ1 - λ2
        a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
        c = 2 ⋅ atan2( √a, √(1−a) )
        d = R ⋅ c
        φ -> latitude 
        λ -> longitude
        R -> 6371
        '''
        
        R = 6371
        delta_lat = lat - lat2
        delta_lon = lon - lon2
        a = pow(sin(toRadians(delta_lat/2)),2) + cos(toRadians(lat)) * cos(toRadians(lat2)) * pow(sin(toRadians(delta_lon/2)),2)
        c = 2 * atan2(pow(a,0.5) , pow(1-a, 0.5) )
        d = R * c
        return d

    spark.udf.register("distance_between_points", distance)



    ## Cross join of data sets 
    loc_poi_temp = location_df_clean.crossJoin(poi_df)



    ## Create a new column for poi
    loc_poi_temp = loc_poi_temp.withColumn('distance', distance(loc_poi_temp['Latitude'], loc_poi_temp['Longitude'], loc_poi_temp['Lat'], loc_poi_temp['Lon'] ) ) 


    ## Select minimum distance 

    w = Window.partitionBy('id','timeSt')
    loc_poi = loc_poi_temp.withColumn('minDistance', f.min('distance').over(w))\
        .where(f.col('distance') == f.col('minDistance'))\
        .drop('minDistance')

    
    
    ### 3. Analysis


    ## Calculate std and mean 
    std_mean = loc_poi.groupBy('POIID').agg(f.avg('distance').alias('mean'),f.stddev('distance').alias('Std'))
    std_mean.show()

    ## Calculate density
    max_count = loc_poi.groupBy('POIID').agg(f.max('distance').alias('Max'),f.count('distance').alias('Count'))
    max_count = max_count.withColumn('density',max_count['Count']/pow(max_count['Max'],2)*math.pi)  
    max_count.show()


    