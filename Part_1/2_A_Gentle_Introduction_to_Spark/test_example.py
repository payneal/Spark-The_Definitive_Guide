# test 
from pyspark import  SparkConf
from pyspark.sql import SparkSession
import unittest
from pyspark.sql.functions import desc
from pyspark.sql.functions import max


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
       
        #  test has nothing to do with partitions except showing one how to
        # not really testing but reference

        conf = SparkConf()
        conf.set("spark.app.name", "test_2")
        conf.set("spark.sql.shuffle.partitions", "5")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
        flightData2015 = spark.read.option(
            "inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
        #  sorting by count
        df = flightData2015.sort('count').take(2)
        self.assertEqual(df[0]['count'], 1)     
        self.assertEqual(str(type(df)), "<type 'list'>")
        self.assertEqual(flightData2015.count(), 256)
        spark.stop()

    def test_make_df_to_table_view(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_3")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option(
            "inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
        flightData2015.createOrReplaceTempView("flight_data_2015")
        sqlWay = spark.sql("Select * FROM  flight_data_2015")
        self.assertEqual(flightData2015.count(), sqlWay.count())
        spark.stop()

    def test_using_max_function(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_3")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option(
            "inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
        one = flightData2015.select(max("count")).take(1)
        flightData2015.createOrReplaceTempView("flight_data_2015")
        two = spark.sql("Select max(count) from flight_data_2015").take(1)
        self.assertEqual(one, two)
        spark.stop()

    def test_destination_total_decrease(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_3")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option(
            "inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")
        df = flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count")\
            .withColumnRenamed("sum(count)", "destination_total")\
            .sort(desc("destination_total")).limit(5).collect()
        self.assertGreater(df[0]['destination_total'], df[1]['destination_total'])
        spark.stop()

    
    def test_sql_vs_data_frame(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_3")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        flightData2015 = spark.read.option(
            "inferSchema", "true").option("header", "true")\
            .csv("./data/2015-summary.csv")

        one = flightData2015.groupBy("DEST_COUNTRY_NAME")\
            .sum("count").withColumnRenamed("sum(count)", "destination_total")\
            .sort(desc("destination_total")).limit(5)
    
        flightData2015.createOrReplaceTempView("flight_data_2015")
        maxSql = spark.sql("""
            SELECT DEST_COUNTRY_NAME, sum(count) as destination_total 
            FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER 
            BY sum(count) DESC LIMIT 5""")
        self.assertEqual(one.first(), maxSql.first())
        spark.stop()

if __name__ == "__main__":
    unittest.main()

