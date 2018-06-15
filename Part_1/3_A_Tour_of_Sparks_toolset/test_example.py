# test
from pyspark import SparkConf
from pyspark.sql  import SparkSession
import unittest
from pyspark.sql.functions import window, column, desc, col
from datetime import timedelta
from pyspark.sql.types import *
import time

class test_chapter_3(unittest.TestCase):
    
    def setUp(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_ch_3")
        # set 5 partitions due to on local machine
        conf.set("spark.sql.shuffle.partitions", "5")
        self.spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
        self.staticSchema = StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", TimestampType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", DoubleType(), True),
            StructField("Country", StringType(), True)
        ])
        
    def tearDown(self):
        pass


    def get_stream(self):
        return self.spark.readStream\
            .schema(self.staticSchema)\
            .option("maxFilesPerTrigger", 1)\
            .format("csv")\
            .option("header", "true")\
            .load("/data/retail-data/by-day/*.csv")

    def test_example_of_of_using_read(self):
        staticDataFrame = self.spark.read.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/*.csv")
        staticDataFrame.createOrReplaceTempView("retail_data")
        #  this is for getting the scheama for streaming example
        staticSchema = staticDataFrame.schema
        self.assertEqual(
            staticSchema[0].jsonValue()['name'], 
            staticDataFrame.columns[0])
    
    def test_window_functions(self):
        staticDataFrame = self.spark.read.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/*.csv")
        df = staticDataFrame.selectExpr(
            "CustomerId", 
            "(UnitPrice * Quantity) as total_cost", 
            "InvoiceDate").groupBy(    
                col("CustomerId"), window(col("InvoiceDate"), "1 day")
            ).sum("total_cost")
        info = df.limit(2).collect()
        self.assertNotEqual(info[0]['CustomerId'], info[1]['CustomerId'])
        # umm.show(20, False) 
        start = info[0]['window']['start'] 
        end = info[0]['window']['end'] 
        # test explains how window function worked in this example
        self.assertEqual(start + timedelta(days=1), end)

    
    def test_example_of_using_streams(self):
        streamingDataFrame = self.get_stream()
        # use this to see if data frame is streaming
        self.assertEqual(streamingDataFrame.isStreaming, True) 
    
    def test_streaming_with_summation(self):
        streamingDataFrame = self.get_stream()
        
        
        print "is streamingDataFram  streaming: {}".format(
            streamingDataFrame.isStreaming)

        

        purchaseByCustomerPerHour = streamingDataFrame\
            .selectExpr( 
                "CustomerId",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate").groupBy(
                    col("CustomerId"), window(col("InvoiceDate"), "1 day")
                ).sum("total_cost")

        umm = purchaseByCustomerPerHour.writeStream\
            .format("memory")\
            .queryName("customer_purchases")\
            .outputMode("complete")\
            .start()
    
        time.sleep(15)


        umm.stop()

        print  "this is umm: "
        print umm

        
        self.spark.sql("""
            Select * 
            FROM customer_purchases
            Order by `sum(total_cost)` DESC
            """).show(5)
        
        self.assertEqual(True, False)


if __name__ == "__main__": 
    unittest.main()
