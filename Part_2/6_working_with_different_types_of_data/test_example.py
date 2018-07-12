# test
import unittest 
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col


class test_chapter_6(unittest.TestCase): 

    def setUp(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_ch_6")
        conf.set(
            "spark.sql.shuffle.partitions", "5")
        self.spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
        self.df = self.spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("./data/retail-data/by-day/2010-12-01.csv")
      
    def tearDown(self):
        self.spark.stop()

    def test_looking_at_lit_function(self):
        self.df.printSchema()
        self.df.createOrReplaceTempView("dfTable")
        self.spark.sql("show tables").show()
        five_df = self.df.select(lit(5), lit("five"), lit(5.0)).first()
        self.assertEqual(five_df[0], 5 )
        self.assertEqual(five_df[1], "five")
        self.assertEqual(five_df[2], 5.0)

    def test_working_with_booleans(self):

        info_df = self.df.where(col("InvoiceNo") != 536365)\
            .select("InvoiceNo", "Description").limit(5).collect()
     
        for x in info_df:
            self.assertNotEqual(x['InvoiceNo'], 536365)

        info_df_1 = self.df.where("InvoiceNo = 536365").limit(5).collect()
        print "df 1 = "
        print info_df_1

        info_df_2 = self.df.where("InvoiceNo <> 536365").limit(5).collect()
        print "df 2 = "
        print info_df_2


if __name__ == "__main__":
    unittest.main()
