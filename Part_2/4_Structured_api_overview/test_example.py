import unittest
from pyspark import SparkConf
from pyspark.sql  import SparkSession
import unittest
from pyspark.sql.types import *


class test_chapter_4(unittest.TestCase):
    
    def setUp(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_ch_4")
        conf.set(
            "spark.sql.shuffle.partitions", "5")
        self.spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
       
    def tearDown(self):
        self.spark.stop()
        
    def test_check_out_spark_types(self):
        self.assertEqual(str(ByteType()), "ByteType")
        self.assertEqual(str(ShortType()), "ShortType")
        self.assertEqual(str(IntegerType()), "IntegerType")
        self.assertEqual(str(LongType()), "LongType")
        self.assertEqual(str(FloatType()), "FloatType")
        self.assertEqual(str(DoubleType()), "DoubleType")
        self.assertEqual(str(DecimalType()), "DecimalType(10,0)")
        self.assertEqual(str(StringType()), "StringType")
        self.assertEqual(str(BinaryType()), "BinaryType")
        self.assertEqual(str(BooleanType()), "BooleanType")
        self.assertEqual(str(TimestampType()), "TimestampType")
        self.assertEqual(str(DateType()), "DateType")
        self.assertEqual(
            str(MapType(StringType(), IntegerType())), 
            "MapType(StringType,IntegerType,true)")
        self.assertEqual(str(StructType()), "StructType(List())")
        self.assertEqual(
            str(StructField("InvoiceNo", StringType(), True)), 
            "StructField(InvoiceNo,StringType,true)")
    
if __name__ == "__main__":
    unittest.main()
