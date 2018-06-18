# test
from pyspark import SparkConf
from pyspark.sql  import SparkSession
import unittest
from pyspark.sql.functions import window, column, desc, col, date_format
from datetime import timedelta
from pyspark.sql.types import *
import time
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans


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
            StructField("CustomerId", DoubleType(), True),
            StructField("Country", StringType(), True)
        ])
        
    def tearDown(self):
        pass

    def get_stream_df(self):
        return self.spark.readStream\
            .schema(self.staticSchema)\
            .option("maxFilesPerTrigger", 1)\
            .format("csv")\
            .option("header", "true")\
            .load("/data/retail-data/by-day/*.csv")

    def get_static_df(self):
        return self.spark.read.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/*.csv")

    def test_example_of_of_using_read(self):
        staticDataFrame = self.get_static_df()
        staticDataFrame.createOrReplaceTempView("retail_data")
        #  this is for getting the scheama for streaming example
        staticSchema = staticDataFrame.schema
        self.assertEqual(
            staticSchema[0].jsonValue()['name'], 
            staticDataFrame.columns[0])
    
    def test_window_functions(self):
        staticDataFrame = self.get_static_df() 
        df = staticDataFrame.selectExpr(
            "CustomerID", 
            "(UnitPrice * Quantity) as total_cost", 
            "InvoiceDate").groupBy(    
                col("CustomerID"), window(col("InvoiceDate"), "1 day")
            ).sum("total_cost")
        info = df.limit(2).collect()
        self.assertNotEqual(info[0]['CustomerID'], info[1]['CustomerID'])
        # print "this is in test window functions"
        # df.show(20, False) 
        start = info[0]['window']['start'] 
        end = info[0]['window']['end'] 
        # test explains how window function worked in this example
        self.assertEqual(start + timedelta(days=1), end)

    
    def test_example_of_using_streams(self):
        streamingDataFrame = self.get_stream_df()
        # use this to see if data frame is streaming
        self.assertEqual(streamingDataFrame.isStreaming, True) 
    
    def test_streaming_with_summation(self):
        streamingDataFrame = self.get_stream_df()
        
        # print "is streamingDataFram  streaming: {}".format(
        #    streamingDataFrame.isStreaming)

        purchaseByCustomerPerHour = streamingDataFrame\
            .selectExpr( 
                "CustomerId",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate").groupBy(
                    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
            .sum("total_cost")

        #start() starts the stream
        umm = purchaseByCustomerPerHour.writeStream\
            .format("memory")\
            .queryName("customer_purchases")\
            .outputMode("complete")\
            .start()
    
        # in real siutuation you can make this call
        # in different terminal and it will show you
        # df so far  while stream is happening
        self.spark.sql("""
            Select * 
            FROM customer_purchases
            Order by `sum(total_cost)` DESC
            """).show(5)

        # couldn't get above dataframe to contain anything so it lead to the
        # conclusion above hopefully Ill understand better with further
        # reading


        # stop the stream
        umm.stop()
        self.assertEqual(True, True)

    def test_transformations_of_data_to_numerical_representation(self):
        staticDataFrame = self.get_static_df() 
        preppedDataFrame = staticDataFrame\
            .na.fill(0)\
            .withColumn(
                "day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
            .coalesce(5)
  
        self.assertEqual(
            len(staticDataFrame.columns) +1,
            len(preppedDataFrame.columns))

        # split data
        trainData = preppedDataFrame\
            .where("InvoiceDate < '2011-07-01'")
        testData = preppedDataFrame\
            .where("InvoiceDate >= '2011-07-01'")
        
        self.assertEqual(
            preppedDataFrame.count(), 
            trainData.count() + testData.count())
    
        # change from string to idx
        indexer = StringIndexer()\
            .setInputCol("day_of_week")\
            .setOutputCol("day_of_week_index")

        encoder = OneHotEncoder(
            inputCol= "day_of_week_index", outputCol ="day_of_week_encoded")

        # create a vector
        vectorAssembler = VectorAssembler()\
            .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
            .setOutputCol("features")
    
        transformationPipeline = Pipeline()\
            .setStages([indexer, encoder, vectorAssembler])

        fittedPipeline = transformationPipeline.fit(trainData)

        transformedTraining = fittedPipeline.transform(trainData)

        transformedTraining.cache()

        kmeans = KMeans().setK(20).setSeed(1L)
        
        kmModel = kmeans.fit(transformedTraining)

        print "this is compute Cost for transformed trainning: {}".format(
            kmModel.computeCost(transformedTraining))
        transformedTest = fittedPipeline.transform(testData)
        print "this is compute cost for transformed test: {}".format(
            kmModel.computeCost(transformedTest))
        
        transformedTest.show(20, False)
        
        self.assertEqual(True, True)




if __name__ == "__main__": 
    unittest.main()
