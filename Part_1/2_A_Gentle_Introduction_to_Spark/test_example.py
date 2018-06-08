# test 
from pyspark import  SparkConf
from pyspark.sql import SparkSession
import unittest

class Test_example(unittest.TestCase):

    def setUp(self):
    # create a single node Spark application
        pass

    def tearDown(self):
        pass

    def test_looking_at_actions(self):

        conf = SparkConf()
        conf.set("spark.app.name", "test_1")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option("inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
    
        # flightData2015.sort('count').explain()
        
	# ex response
	# == Physical Plan == 
	# *Sort [count#195 ASC NULLS FIRST], true, 0
	# +- Exchange rangepartitioning(count#195 ASC NULLS FIRST, 200)   
	# 	+- *FileScan csv [DEST_COUNTRY_NAME#193,ORIGIN_COUNTRY_NAME#194,count#195] ...  

	flightData2015 = flightData2015.take(3)
        self.assertEqual(len(flightData2015),3)
        spark.stop()

    def test_changing_partitions(self):
        
        conf = SparkConf()
        conf.set("spark.app.name", "test_2")
        conf.set("spark.sql.shuffle.partitions", "5")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option("inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
   
        df = flightData2015.sort('count').take(2)
        
        print "this is df = {}".format(df)
        print "this is type(df)= {}".format(type(df))
        
        spark.stop()

        
if __name__ == "__main__":
    unittest.main()

