# Product Name

TECHNICAL SKILLS ASSESSMENT 
EQ Works - Data Engineer

## Setup 

After cloning the present repository, follow the steps detailed in the "Setup" section of the repository https://github.com/EQWorks/ws-data-spark.git

## Description 

Three phases are executed where the following operations are applied 

1. Cleanup

Clean-up of suspicious request records. We consider suspicious records the ones that have identical geoinfo and timest. 

2. Label

Assign each request (from data/DataSample.csv) to the closest (i.e. minimum distance) POI (from data/POIList.csv).

3. Analysis

For each POI, calculate the average and standard deviation of the distance between the POI to each of its assigned requests.

Calculate the radius and density (requests/area) for each POI.




## Usage example

After following the steps detailed in 'Setup' section an UI wiht the command line inside the container will be available.
This will be used to submit the spark jobs.

From a new terminal window execute the following command to copy the script into the container: 
```bash
docker cp data_processing_script.py ws-data-spark_master_1:/data_processing_script.py
```

Note: replace ws-data-spark_master_1 with the name of the container being used 


After creating a copy of the script in the container execute the following command in the command line corresponding to the container 
```bash
spark-submit /data_processing_script.py
```
The resulting tables are defined in Description.Analysis, they will look as follows:

```bash
+-----+------------------+------------------+
|POIID|              mean|               Std|
+-----+------------------+------------------+
| POI4|  514.997132159558|1506.8900004967763|
| POI2|300.71466043948016| 388.2733899554214|
| POI1|300.71466043948016| 388.2733899554214|
| POI3|451.65111198840964|223.63173939154564|
+-----+------------------+------------------+

+-----+------------------+-----+--------------------+
|POIID|               Max|Count|             density|
+-----+------------------+-----+--------------------+
| POI4|  9349.57272915129|  422|1.516627041109484...|
| POI2|11531.820906116562| 8749|2.066866904554495E-4|
| POI1|11531.820906116562| 8749|2.066866904554495E-4|
| POI3|1474.5808350038258| 8802|0.012717275118819583|
+-----+------------------+-----+--------------------+
```
When 2 POI are located at the exact same location request are assigned two both POI.
More information about the requests and POI would be necessary to choose which one should be assigned 

## Meta

Bernardo Sandi – bsandi1220@gmail.com



