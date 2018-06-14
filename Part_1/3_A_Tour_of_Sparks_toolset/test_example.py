# test
from pyspark import SparkConf
from pyspark.sql  import SparkSession
import unittest
from pyspark.sql.functions import window, column, desc, col


class test_chapter_3(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_example_of_streaming(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_streaming")
        spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
        staticDataFrame = spark.read.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/*.csv")
        staticDataFrame.createOrReplaceTempView("retail_data")
        staticSchema = staticDataFrame.schema
        self.assertEqual(
            staticSchema[0].jsonValue()['name'], 
            staticDataFrame.columns[0])
    
    def test_window_functions(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_window_function")
        spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
        staticDataFrame = spark.read.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/*.csv")
      
        umm = staticDataFrame.selectExpr(
            "CustomerId", 
            "(UnitPrice * Quantity) as total_cost", 
            "InvoiceDate").groupBy(    
                col("CustomerId"), window(col("InvoiceDate"), "2 week"))\
            .sum("total_cost")
        
        umm.show(20, False)

        self.assertEqual(True, False)


if __name__ == "__main__": 
    unittest.main()
