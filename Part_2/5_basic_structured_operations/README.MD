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

        ``
        


