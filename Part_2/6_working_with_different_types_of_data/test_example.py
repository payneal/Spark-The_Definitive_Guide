# test
import unittest 
import math
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, instr, expr, round, bround, corr, monotonically_increasing_id, translate

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
        # self.df.printSchema()
        self.df.createOrReplaceTempView("dfTable")
        five_df = self.df.select(lit(5), lit("five"), lit(5.0)).first()
        self.assertEqual(five_df[0], 5 )
        self.assertEqual(five_df[1], "five")
        self.assertEqual(five_df[2], 5.0)

    def test_working_with_booleans(self):

        info_df = self.df.where(col("InvoiceNo") != 536365)\
            .select("InvoiceNo", "Description").limit(5).collect()
     
        for x in info_df:
            self.assertNotEqual(int(x['InvoiceNo']), 536365)

        info_df_1 = self.df.where("InvoiceNo = 536365").limit(5).collect()

        for x in info_df_1:
            self.assertEqual(int(x['InvoiceNo']), 536365)

        info_df_2 = self.df.where(
                "InvoiceNo <> 536365").limit(100).collect()
        for x in info_df_2:
            self.assertGreaterEqual(int(x['InvoiceNo']), 536365)

    def test_working_wit_filters(self):
        priceFilter = col("UnitPrice") > 600 
        descripFilter = instr(self.df.Description, "POSTAGE") >=1
        df = self.df.where(self.df.StockCode.isin("DOT")).where(
            priceFilter | descripFilter)
        self.assertEqual(df.count(), 2)
    
    def test_or_statement_with_filter(self): 
        price_filter = col('UnitPrice') > 600
        descrip_filter = instr(self.df.Description, "POSTAGE") >= 1 
        filtered_df = self.df.where(self.df.StockCode.isin("DOT")).where(
            price_filter | descrip_filter)
        self.assertEqual(2, filtered_df.count())

    def test_booleans_on_columns(self):
        DOT_code_filter = col("StockCode") == "DOT"
        price_filter = col('UnitPrice') > 600
        description_filter = instr(col("Description"), "POSTAGE") >= 1
        new_df = self.df.withColumn(
            "isExpensive", DOT_code_filter & (price_filter | description_filter))\
            .where("isExpensive").select("unitPrice", "isExpensive").collect()
        for x in new_df: 
            self.assertEqual(x['isExpensive'], True)
    
    def test_using_expressions_with_filtering(self):
        new_df = self.df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
            .where("isExpensive")\
            .select("Description", "UnitPrice").collect()
        for x in new_df:
            self.assertEqual(x['Description'], "DOTCOM POSTAGE")
    

    def test_working_with_numbers(self):
	fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
	new_df = self.df.select(
    	    expr("CustomerId"), 
            fabricatedQuantity.alias("realQuantity"),
            expr('Quantity'),
            expr('UnitPrice'))

        new_df_list = new_df.collect()

        for x in new_df_list:
            self.assertEqual(
                x['realQuantity'], 
                math.pow(x['Quantity'] * x['UnitPrice'], 2) + 5) 
    

        df_with_expression = self.df.selectExpr(
            "CustomerId",
            "(Power((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
    

        df_with_expression = df_with_expression.collect()
        new_df = new_df.collect()
        

        for x in range(0, 5):
            self.assertEqual(
                df_with_expression[x]["CustomerId"],
                new_df[x]['CustomerId'])
            self.assertEqual(
                df_with_expression[x]["realQuantity"],
                new_df[x]['realQuantity'])

    def test_round_in_pyspark(self):
        df = self.df.select(
            round(lit("2.5")), 
            bround(lit("2.5"))).collect()
 
        for x in df:
            self.assertEqual(x['round(2.5, 0)'], 3.0)
            self.assertEqual(x['bround(2.5, 0)'], 2.0)
    
    def test_correlation_in_spark(self):
        self.df.stat.corr("Quantity","UnitPrice")
        
        # shows all data
        # self.df.show()

        umm = self.df.select(
                corr("Quantity", "UnitPrice")
            ).collect()[0]['corr(Quantity, UnitPrice)']
        
        #  negitive correlation
        self.assertLess(umm, 0)


    def test_look_at_stats (self):
        #self.df.describe().show()
        self.assertEqual(True, True)


    def test_calculate_exact_or_approximate_quantiles(self):
        colName = "UnitPrice"
        quantileProbs = [0.5]
        relError = 0.05
        middle_quanti =  self.df.stat.approxQuantile(
            "UnitPrice", quantileProbs, relError)
        low_quanti =  self.df.stat.approxQuantile(
            "UnitPrice", [0.1], 0.05)
        high_quanti =  self.df.stat.approxQuantile(
            "UnitPrice", [0.9], 0.05)
        self.assertLess(low_quanti, middle_quanti)
        self.assertLess(middle_quanti, high_quanti)


    def test_crosstab(self):
        df = self.df.stat.crosstab(
            "StockCode", "Quantity").take(10)
        
        # print  "below is the df in a list"
        # print df

        df = self.df.stat.freqItems(
            ["StockCode", "Quantity"]).take(10)
        # print "what is this: {}".format(df)
        # idk how to test this yet so keep trying
        self.assertEqual(True, True)

    def test_adding_monotoniucally_increasing_id(self):
        
        
        df_with_increasing_id = self.df.select(
            monotonically_increasing_id()).limit(5).collect()
        count = 0

        for x in df_with_increasing_id:
            self.assertEqual(
               x['monotonically_increasing_id()'], count)
            count += 1
    
        
    


if __name__ == "__main__":
    unittest.main()
