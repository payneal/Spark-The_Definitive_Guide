import unittest
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, LongType
import unittest

class test_chapter_5(unittest.TestCase):
    
    def setUp(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_ch_5")
        conf.set(
            "spark.sql.shuffle.partitions", "5")
        self.spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
       
    def tearDown(self):
        self.spark.stop()
        
    def test_data_frame(self):
        df = self.spark.read.format("json")\
            .load("./data/flight-data/json/2015-summary.json")
        self.assertEqual(
            str(type(df)), 
            "<class 'pyspark.sql.dataframe.DataFrame'>")

    def test_check_out_df_schema(self):
        schema = self.spark.read.format("json").load(
            "./data/flight-data/json/2015-summary.json").schema
        self.assertEqual(str(schema), 
            "StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),StructField(ORIGIN_COUNTRY_NAME,StringType,true),StructField(count,LongType,true)))")

    def test_check_cols(self):
        df = self.spark.read.format("json")\
            .load("./data/flight-data/json/2015-summary.json")
        self.assertIn("count", df.columns)
    
    def test_data_frame_row(self):
        df = self.spark.read.format("json")\
            .load("./data/flight-data/json/2015-summary.json")
        first_row = df.collect()[0]
        row = df.first()
        self.assertEqual(row, first_row)

    def test_creating_a_data_frame(self):
        myManualSchema = StructType([
            StructField( "some", StringType(), True),
            StructField("col", StringType(), True), 
            StructField("names", LongType(), True) 
        ])
        
        myRow = Row("Hello", None, 1 ) 
        myDF = self.spark.createDataFrame( [myRow ],  myManualSchema)
        self.assertListEqual(['some', 'col', 'names'], myDF.columns) 


if __name__ == "__main__":
    unittest.main()
