import unittest
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr, col, column, lit, desc, asc
from pyspark.sql.types import StructField, StructType, StringType, LongType

class test_chapter_5(unittest.TestCase):
    
    def setUp(self):
        conf = SparkConf()
        conf.set("spark.app.name", "test_ch_5")
        conf.set(
            "spark.sql.shuffle.partitions", "5")
        self.spark = SparkSession.builder.config(
            conf=conf).getOrCreate()
        self.df = self.spark.read.format("json")\
            .load("./data/flight-data/json/2015-summary.json")
      
    def tearDown(self):
        self.spark.stop()
        
    def test_data_frame(self):
        self.assertEqual(
            str(type(self.df)), 
            "<class 'pyspark.sql.dataframe.DataFrame'>")

    def test_check_out_df_schema(self):
        schema = self.df.schema 
        self.assertEqual(str(schema), 
            "StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),StructField(ORIGIN_COUNTRY_NAME,StringType,true),StructField(count,LongType,true)))")

    def test_check_cols(self):
        self.assertIn("count", self.df.columns)
    
    def test_data_frame_row(self):
        first_row = self.df.collect()[0]
        row = self.df.first()
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

    def test_using_select(self):
        dest_country_name_df = self.df.select("DEST_COUNTRY_NAME")
        dest_country_and_origin_country_df = self.df.select("DEST_COUNTRY_NAME",  "ORIGIN_COUNTRY_NAME")
    
        self.assertEqual(
            dest_country_name_df.collect()[0]["DEST_COUNTRY_NAME"],
            dest_country_and_origin_country_df.collect()[0]['DEST_COUNTRY_NAME'])
        
        self.assertNotEqual(
            len(dest_country_name_df.columns), 
            len(dest_country_and_origin_country_df.columns))

    def test_using_select_expr(self):
        df = self.df.select(
            expr("DEST_COUNTRY_NAME"),
            col("DEST_COUNTRY_NAME"),
            column("DEST_COUNTRY_NAME"))
        self.assertListEqual(
            df.columns, 
            ['DEST_COUNTRY_NAME', 'DEST_COUNTRY_NAME', 'DEST_COUNTRY_NAME']) 

    def test_table_name_changing_with_expr(self):
        df = self.df.select(expr("DEST_COUNTRY_NAME as destination"))
        self.assertListEqual(df.columns, ['destination'])

    def test_table_name_double_change(self):
        df = self.df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
        self.assertListEqual(df.columns, ['DEST_COUNTRY_NAME'])

    def test_table_name_with_selectExpr(self):
        df = self.df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME")
        self.assertListEqual(df.columns, ["newColumnName", "DEST_COUNTRY_NAME"])

    def test_adding_cols_with_select_expr_that_include_all_original(self):
        df = self.df.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
        self.assertListEqual(df.columns, ["DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count", "withinCountry"])

    def test_select_expression_with_aggregations(self):
        df = self.df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")
        self.assertEqual(df.collect()[0]['count(DISTINCT DEST_COUNTRY_NAME)'], 132)

    def test_literals(self):
        df = self.df.select(expr("*"), lit(1).alias("One"))
        self.assertListEqual(df.columns, ["DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count", "One"])

    def test_adding_columns_withColumn(self):
        df = self.df.withColumn("numberOne", lit(1))
        self.assertListEqual(df.columns, ['DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME', 'count', 'numberOne'])
    

    def test_makeing_an_actual_expression(self):
        df = self.df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
        cond_1 = col("DEST_COUNTRY_NAME") == 'United States'
        cond_2 = col("ORIGIN_COUNTRY_NAME") == 'United States' 
        df = df.where(cond_1 & cond_2)
        self.assertEqual(df.collect()[0]['withinCountry'], True)
    
    def test_renaming_cols_withColumnRenamed(self):
        new_df = self.df.withColumnRenamed("DEST_COUNTRY_NAME", 'dest')
        self.assertNotIn('dest', self.df.columns)
        self.assertIn("dest", new_df.columns)
    
    def test_creating_a_column_with_reserved_chars(self):
        df_with_long_col_change = self.df.withColumn("this Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
        self.assertNotIn('this Long Column-Name', self.df.columns)
        self.assertIn("this Long Column-Name", df_with_long_col_change.columns)
        new_df = df_with_long_col_change.selectExpr(
            "`This Long Column-Name`",
            "`This Long Column-Name` as `new col`")
        self.assertIn('new col',new_df.columns)
        df_with_long_col_change.createOrReplaceTempView("dfTableLong")
        df_from_sql = self.spark.sql("select * from dfTableLong limit 2")
        self.assertListEqual(
            ['DEST_COUNTRY_NAME', "ORIGIN_COUNTRY_NAME", "count", "this Long Column-Name"],
            df_from_sql.columns        
        )
        
        self.assertListEqual(
            df_with_long_col_change.select(expr("`This Long Column-Name`")).columns, 
            ['This Long Column-Name'])

    def test_removing_cols(self): 
        df = self.df.drop("ORIGIN_COUNTRY_NAME")
        df = df.drop("count", "DEST_COUNTRY_NAME")
        self.assertEqual(df.columns, [])

    def test_type_casting(self):
        df = self.df.withColumn("count2", col("count").cast("long"))
        self.assertEqual(df.dtypes[3][1], "bigint")
    
    def test_filter_and_where(self):
        # filter and where are the same 
        df = self.df.filter(col("count") < 2)
        df2 = self.df.where(col("count") < 2)
        self.assertListEqual(df.collect(), df2.collect())
        self.assertNotEqual(self.df.collect()[0]['count'], df.collect()[0]['count'])

    def test_filter_multiple_example(self):
        df = self.df.where( col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") == "Croatia")
        self.assertEqual(df.collect()[0]['ORIGIN_COUNTRY_NAME'], "Croatia")

    def test_unique_rows(self):
        init_count = self.df.count()
        count_distinct_country_dest = self.df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
        self.assertEqual(init_count, count_distinct_country_dest)
        count_origin = self.df.select("ORIGIN_COUNTRY_NAME").distinct().count()
        self.assertNotEqual(init_count, count_origin)

    def test_random_sample(self):
        seed = 5
        withReplacement= False
        fraction = 0.5
        df = self.df.sample(withReplacement, fraction, seed)
        self.assertEqual(126, df.count())

    def test_random_split(self):
        seed = 5 
        dataFrames = self.df.randomSplit([0.25, 0.75], seed)
        self.assertLess(dataFrames[0].count(), dataFrames[1].count())

    def test_concatenating_and_appending_row(self):
        schema = self.df.schema
        newRows = [
            Row("New Country", "Other Country", 5L),
            Row("New Country 2", "Other Country 3", 1L)
        ]
        parallelizedRows = self.spark.sparkContext.parallelize(newRows)
        newDF = self.spark.createDataFrame(parallelizedRows, schema)
        
        count_before_union = self.df.where("count = 1")\
            .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
            .count()
        count_after_union = self.df.union(newDF)\
            .where("count = 1")\
            .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
            .count()
        self.assertEqual(count_before_union + 1, count_after_union)
        
    def test_sortin_rows(self):
        #sort and orderBy work the exact same way
        count_df = self.df.sort("count")
        order_first_way = self.df.orderBy("count", "DEST_COUNTRY_NAME")
        order_second_way = self.df.orderBy(col("count"), col("DEST_COUNTRY_NAME"))
        self.assertListEqual(order_first_way.collect(), order_second_way.collect())
        self.assertNotEqual(count_df.collect(), order_first_way.collect())

    def test_sorting_with_direction(self):
        dec = self.df.orderBy(expr("count desc"))
        asc = self.df.orderBy(
            col("count").desc(), col("DEST_COUNTRY_NAME").asc())
        self.assertEqual(dec.count(), asc.count())
        self.assertEqual(
            dec.collect()[dec.count()-1], 
            asc.collect()[0])

    def test_limiting_dataframes(self):
        df = self.df.limit(5)
        df_2 = self.df.orderBy(expr("count desc")).limit(6)

        self.assertEqual(df.count(), 5)
        self.assertEqual(df_2.count(), 6)

        self.assertLessEqual(
            df_2.collect()[0]['count'],
            df.collect()[0]['count'])

    def test_repartition_and_coalesce(self):
        self.assertEqual(
            self.df.rdd.getNumPartitions(),
            1)
        self.df = self.df.repartition(5)
        self.assertEqual(
            self.df.rdd.getNumPartitions(),
            5)
        self.df = self.df.repartition(
            2, col("DEST_COUNTRY_NAME"))
        self.assertEqual(
            self.df.rdd.getNumPartitions(),
            2)
        self.df = self.df = self.df.repartition(
            3, col("DEST_COUNTRY_NAME")).coalesce(1)
        self.assertEqual(
            self.df.rdd.getNumPartitions(),
            1)

    def test_collect_vs_localIterator(self):
        collect = self.df.limit(5).collect()
        local_collect = self.df.limit(5).toLocalIterator()
        for x in local_collect: 
             self.assertIn(x, collect)

if __name__ == "__main__":
    unittest.main()
