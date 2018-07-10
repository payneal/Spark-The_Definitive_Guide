# chapter 5 basic structured operations
* DataFrame consists of a series of records (like rows in a table), that are of type Row, and a number of columns (like columns in a spreadsheet) that represent a computation expression that can be performed on each individual record in the Dataset
* Schemas define the name as well as the type of data in each column.
* Partitioning of the DataFrame defines the layout of the DataFrame or Dataset’s physical distribution across the cluster
* The partitioning scheme defines how that is allocated. You can set this to be based on values in a certain column or nondeterministically.Let’s create a DataFrame with which we can work

* create a data frame
```python
    df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
```

* look  at the schema on a data frame
```python
    df.printSchema()
```

## schemas
* A schema defines the column names and types of a DataFrame. We can either let a data source define the schema (called schema-on-read) or we can define it explicitly ourselves.
 
 * important
 * Deciding whether you need to define a schema prior to reading in your data depends on your use case. For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with plain-text file formats like CSV or JSON). However, this can also lead to precision issues like a long type incorrectly set as an integer when reading in a file. When using Spark for production Extract, Transform, and Load (ETL), it is often a good idea to define your schemas manually, especially when working with untyped data sources like CSV and JSON because schema inference can vary depending on the type of data that you read in.
 
 * check out schema of data frame
 ```python
 spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema
 
 ```

 * A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean flag which specifies whether that column can contain missing or null values, and, finally, users can optionally specify associated metadata with that column.

* If the types in the data (at runtime) do not match the schema, Spark will throw an error

* ideal production etl with spark 
```python
    from pyspark.sql.types import StructField, StructType, StringType, LongType

    myManualSchema = StructType([  
        StructField("DEST_COUNTRY_NAME", StringType(), True),  
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),  
        StructField("count", LongType(), False, metadata={"hello":"world"})
    ])
    
    df = spark.read.format("json").schema(myManualSchema)\  
        .load("/data/flight-data/json/2015-summary.json")
```

* we cannot simply set types via the per-language types because Spark maintains its own type information.

## Columns and Expressions
* Columns in Spark are similar to columns in a spreadsheet, R dataframe, or pandas DataFrame. You can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions.

 * columns are logical constructions that simply represent a value computed on a per-record basis by means of an expression

 * to have a real value for a column, we need to have a row; and to have a row, we need to have a DataFrame.

* you cannot manipulate an individual column outside the context of a DataFrame; you must use Spark transformations within a DataFrame to modify the contents of a column

* a lot of different ways to construct and refer to columns but the two simplest ways are by using the col or column functions

```python
    from pyspark.sql.functions import col, column
    col("someColumnName")
    column("someColumnName")
```

*  Columns are not resolved until we compare the column names with those we are maintaining in the catalog. Column and table resolution happens in the analyzer phase,

* Expressions
    * An expression is a set of transformations on one or more values in a record in a DataFrame.
    * Think of it like a function that takes as input one or more column names, resolves them, and then potentially applies more expressions to create a single value for each record in the dataset. Importantly, this “single value” can actually be a complex type like a Map or Array
    * an expression, created via the expr function, is just a DataFrame column reference. In the simplest case, expr("someCol") is equivalent to col("someCol")

    * Sometimes, you’ll need to see a DataFrame’s columns, which you can do by using something like printSchema; however, if you want to programmatically access columns, you can use the columns property to see all columns on a DataFrame:
    ```python
        spark.read.format("json").load(
            "/data/flight-data/json/2015-summary.json").columns
    ```
    
    * getting the first row of data
    ```python
        df.first()
    ```
    
* creating rows
    * rtant to note that only DataFrames have schemas. Rows themselves do not have schemas. This means that if you create a Row manually, you must specify the values in the same order as the schema of the DataFrame to which they might be appended 
    ```python
        
        from pyspark.sql import Row 
        myRow  = Row("hello", None, 1, False)
        
    ```
# DataFrame  Transformations
    * there are some fundamental objectives. These break down into several core operations
        * we can add rows or columns
        * we can remove rows or columns
        * we can transform a row intoa column (or vice versa)
        * we can change the orfderofrows based on the values in columns


* creating  dataframes 
    * we will use them now to create an example DataFrame (for illustration purposes later in this chapter, we will also register this as a temporary view so that we can query it with SQL and show off basic transformations in SQL, as well):
    ```python
        
        df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json") 
        df.createOrReplaceTempView("dfTable")
    ```

    * we can also create dataframes on the fly by taking a set of rows and converting them to a data frame

    ```python
        from pyspark.sql import Row
        from pyspark.sql.types import StructField, StructType, StringType, LongType
        myManualSchema = StructType([  
            StructField("some", StringType(), True),  
            StructField("col", StringType(), True),  
            StructField("names", LongType(), False)
        ])
        myRow = Row("Hello", None, 1)
    myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()
    ```

    * now that you know how to create a data frame lets now look at the most useful methods 
        * select method when your working with columns or expressions
        * selectExpr method when your working with expressions in strings
        * some transformations ar not specified as methods on columns therefor there are a group of functions fount in spark.sql.functions

    * with the 3 tools listed abobe you should be able to solve the vast majority of transformations challenges that you might encounter in Data frames

    * select and selectExpr
        * allow you to do the data frame equivalent of sql queries oina table of data
        * in sql
        ```sql
            
            select * from dataFrameTable
            select coloumnName from dataFrameTable
            select columnName * 10, otherColumn, someOtherCol as c From dataFrameTable
        ```

        * you can use them to manipulate columns in your DataFrames. Let’s walk through some examples on DataFrames to talk about some of the different ways of approaching this problem.

        * following are same in python and sql
        ```python
            df.select("DEST_COUNTRY_Name").show(2)
        ```
        ```sql
            select DEST_COUNTRY_Name from dfTable Limit 2
        ```
    
        * You can select multiple columns by using the same style of query, just add more column name strings to your select method call:

        ```python
            df.select("DEST_COUNRTY_NAME", "ORIGIN_COUNTRY_NAME") 
        ```
        
        *  you can refer to columns in a number of different ways:
        * see below: expr(), col(), column()
        ```python
        
            from pyspark.sql.functions import expr, col, column
            
            df.select(
                 expr( "DEST_COUNTRY_NAME"),
                col("DEST_COUNTRY_NAME"),
                column("DEST_COUNTRY_NAME"))
        ```
        
        *  selectExpr is probably the most convenient interface for everyday use:
            ```python
                df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
            ```
            
            * we can treat selectExpr as a simple way of building up complex expressions that create new DataFrames
            ```python
                df.selectExpr(
                    "*", # all original columns 
                    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
            
            ```
            
            * with select expression, we can also specify aggregation over the entire dataframe by taking advantage of the functions that we have
            ```python
                df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
            ```

        * converting to spark types(literals)
            *  sometimes one needs to pass explicit values into Spark that are just a value. this is done via literals
            ```python
            from pyspark.sql.functions import lit
            df.select(expr("*"), lit(1).alias("One")).show(2) 
            ```

        * adding Columns
            * adding a column that just adds the number one as a column
            ```python
                df.withColumn("numberOne", lit(1)).show(2)
            ```

            * setting a boolean flag for when the origin country is the same as the destination country
            ```python
                df.withColumn('withinCountry', expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
            ```
            
            * withColumn function takes 2 args: the colname and the expression that will create the value for that given row in the dataframe

            * also can rename a column with withColumn
            ```python
                df.withColumn('Destination', expr('DEST_COUNTRY_NAME')).columns
            ```

            * Renaming Columns
                * although we can rename column like above you also can with withColumnRenamed
                ```python
                    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
                ```

        * reserved characters and keywords
        * to use reserved characters in column name we must use backtick
        ```python
            df_with_long_col_name.selectExpr(
                "`This Long Column-Name`", 
                "`This Long Column-Name` as `new col`").show(2)
        ```

        * case sensitivity
            * by default spark is case insensitive 
            * to change this: set spark.sql.caseSensitive true

        * removing columns
            * dedicated method to remove columns
            ```python
                df.drop("ORIGIN_COUNTRY_NAME").columns
            ```
            * dropping multiple columns
            ```python
                df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
            ```

        * changing a column's type(cast)
            * convert from one type to another for example if er have a set of StringType that should be integers
            ```python
                df.withColumn("count2", col("count").cast("long"))
            ```
        *  filtering rows
            * to filter rows we create an expression that evaluates to true or false
            * can use filter or where
            ```python
                df.filter(col("count") < 2).show(2)
                df.where("count < 2").show(2)
            ```
        
        * multiple filters into the same expression
            * to specify multiple AND filters , just chain them sequentially and let Spark handle the rest:
            ```python
                df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)
            ```

        * getting  unique rows
            * very common use case is to extract the unique or distinct values in a dataframe
            * do this by using the distinct method on a data frame
            ```python
                df.select("ORIGIN_COUNTRY_NAME",  "DEST_COUNTRY_NAME").distinct().count()
            ```

        * random samples
            *  might  want to sample some random records  from your data frame
            * can do this by using the sample method on a data frame
            ```python
                seed = 5
                withReplacement = false
                fraction = 0.5
                df.sample(withReplacement, fraction, seed).count()
            ```

        * random splits
            * random split can be helpful when you need to break up yourdata frame into random splits of the original dataframe
            * Because this method is designed to be randomized, we will also specify a seed (just replace seed with a number of your choosing in the code block). It’s important to note that if you don’t specify a proportion for each DataFrame that adds up to one, they will be normalized so that they do: 
                ```python
                    dataFrames = df.randomSplit([0.25, 0.75],  seed) 
                    dataFrames[0].count() > dataFrames[1].count() # False
                
                ```

        * Concatenating and appending rows(union)
            * dataframes are immutable
            * therefore users cannot append to data frames because that would be changing it
            * to append to a dataframe you must union the original dataframe along with the new dataframe
            * this just concatenates the two dataframes. to union 2 dataframes, must be sure that they have the same schema and number of columns; otherwise  the union will fail
            ```python
                from pyspark.sql import Row 
                schema = df.schema
                newRows = [
                    Row("New Country", "Other Country", 5L),
                    Row("New Country 2", "Other Country  3", 1L)
                ]
                parallelizedRows = spark.sparkContext.parallelize(newRows)
                newDF = spark.createDataFrame(parallelizedRows, schema)

                df.union(newDF)\
                    .where("count = 1")\
                    .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
                    .show()

        * sorting rows
            * when we sort the values in a data frame, we always want to to sort with either the largest or smallest values at the top of a fataframe
            * sort and orderBy work the exact same way 
            * they accept both column expressions and strings as well as multiple columns
            * the  default is to sort in ascending order
            ```python
                
                df.sort("count").show(5)
                df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
                df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
            ``` 
            * to more explicitly specify sort direction, you need to use the asc and desc functions if operation on a column
            ```python
            
            from pyspark.sql.functions import desc,asc

            df.orderBy(expr("count desc")).show(2)
            df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

            ```

            * advanced tip is to user asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last to specify where you would like your null values to appear in an ordered dataframe

            * for optimization purposem its sometimes advisable to sort within each partition before another set of transformations. you can use the sortWithinPartitions metod to do this
            ```python
                spark.read.format("json").load("/data/flight-data/json/*-summary.json").sortWithinPartitions("count")
            ```

        *  limit
            * you might want to restrict what you extract from a dataframe. you may just want a few entries. use limit method
            ```python
            
                df.limit(5).show()

                df.orderBy(expr("count desc")).limit(6).show() 

            ```

        * repartition and coalesce
            * important optimization opportunity is to partition the data according to some frequently filtered columns, which control the physical layout of data across the cluster including the partitioning scheme and the number of partitions.
            * Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to partition by a set of columns:

            ```python
                
                # get number of partitions
                df.rdd.getNumPartitions() 

                # repartition
                df.repartition(5)
            
            ```

            * if you know that your going to be filtering by a certain column often, it wan be worth repartitioning based on that column:
            ```python
                df.repartition(col("DEST_COUNTRY_NAME"))
            ```
            
            * can do column and number of partitions
            ```python
                df.repartition(5, col("DEST_COUNTRY_NAME"))
            ```

            * Coalesce, on the other hand, will  noy incur a full shuffle and will try to combine partitions
            ```python
                 df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
            ```

        * collecting Rows to the driver
            * spark maintains the state of the cluster in the driver
            * there are times when you will want to collect some of your data to the driver in order to manipulate it on your local machine
            * very similar to collect(get all data from entire dataframe), 
            * take(selects the first N rows), 
            * show prints out a number of rows nicely
            ```python
                
                collectDF = df.limit(10)
                collectDF.take(5) # take works with an Integer count
                collectDF.show()  #  this prints itout nicely
                collectDF.show(5, False) 
                collectDF. collect()
            ```
        
            * additional way of collecting rows to the  driver in order to iterate over the entire dataset 
            * the method toLocalIterator collects partitions to the driver as an iterator
            * this method allows you to iterate over the entire dataset partition-by-partition in a serial manner:
            ```python
                umm = collectDF.toLocalIterator
            
                # go through all
                for x in umm:
                    print x
            ```

            

