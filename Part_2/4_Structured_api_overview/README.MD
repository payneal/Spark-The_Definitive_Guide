# Chapter 4. Structured API Overview
    * apis refer to three core types of distributed collection APIS
        * Datasets
        * DataFrames
        * SQL tables and views

    * Although they are distinct parts of the book, the majority of the Structured APIs apply to both batch and streaming computation
    * when you work with the Structured APIs, it should be simple to migrate from batch to streaming (or vice versa) with little to no effort

## DataFrames and Datasets
    * DataFrames and Datasets are (distributed) table-like collections with well-defined rows and columns. Each column must have the same number of rows as all the other columns (although you can use null to specify the absence of a value) and each column has type information that must be consistent for every row in the collection.
    *  DataFrames and Datasets represent immutable, lazily evaluated plans that specify what operations to apply to data residing at a location to generate some output. When we perform an action on a DataFrame, we instruct Spark to perform the actual transformations and return the result. These represent plans of how to manipulate rows and columns to compute the user’s desired result.
    
    * Tables and views are basically the same thing as DataFrames. We just execute SQL against them instead of DataFrame cod
    
    * To add a bit more specificity to these definitions, we need to talk about schemas, which are the way you define the types of data you’re storing in this distributed collection.

## Schemas
    
    * A schema defines the column names and types of a DataFrame. You can define schemas manually or read a schema from a data source (often called schema on read). Schemas consist of types, meaning that you need a way of specifying what lies where

    * Spark is effectively a programming language of its own
    * Spark uses an engine called Catalyst that maintains its own type information through the planning and processing of work
    * Spark types map directly to the different language APIs that Spark maintains and there exists a lookup table for each of these in Scala, Java, Python, SQL, and R. Even if we use Spark’s Structured APIs from Python or R, the majority of our manipulations will operate strictly on Spark types, not Python types
    * following code does not perform addition in Scala or Python; it actually performs addition purely in Spark:

    ```python
        df = spark.range(500).toDF("number")
        df.select(df["number"] + 10)
    ```
    * Spark will convert an expression written in an input language to Spark’s internal Catalyst representation of that same type information. It then will operate on that internal representation.

    * DataFrames Vs Datasets
        * within the Structured APIs, there are two more APIs, the “untyped” DataFrames and the “typed” Datasets
        * DataFrames are untyped is aslightly inaccurate; they have types, but Spark maintains them completely and only checks whether those types line up to those specified in the schema at runtime
        * Datasets, on the other hand, check whether types conform to the specification at compile time. Datasets are only available to Java Virtual Machine (JVM)–based languages (Scala and Java) and we specify types with case classes or Java beans.

        * To Spark (in Scala), DataFrames are simply Datasets of Type Row. The “Row” type is Spark’s internal representation of its optimized in-memory format for computation. This format makes for highly specialized and efficient computation because rather than using JVM types, which can cause high garbage-collection and object instantiation costs, Spark can operate on its own internal format without incurring any of those costs. To Spark (in Python or R), there is no such thing as a Dataset: everything is a DataFrame and therefore we always operate on that optimized format
    
    * Columns
        * Columns represent a simple type like an integer or string, a complex type like an array or map, or a null value.

    * Rows
        * A row is nothing more than a record of data. Each record in a DataFrame must be of type Row, as we can see when we collect the following DataFrames
        ```python
            spark.range(2).collect()
        ```

    * Spark Types
        ```python
            from pyspark.sql.types import *
            b = ByteType()
        ```
    
    ![SparkTypes](/./images/python_spark_types_1.PNG?raw=true "SparkTypes")
    ![SparkTypes](/./images/python_spark_types_2.PNG?raw=true "SparkTypes")
    ![SparkTypes](/./images/python_spark_types_3.PNG?raw=true "SparkTypes")

    * you almost never work with purely static DataFrames. You will always manipulate and transform them.

#  Overview of Structured API Execution
* Walk through the execution of a single structured API query from user code to executed code. Here’s an overview of the steps:
    1. Write DataFrame/Dataset/SQL Code.
    2. If valid code, Spark converts this to a Logical Plan.
    3. Spark transforms this Logical Plan to a Physical Plan, checking for optimizations along the way.
    4. Spark then executes this Physical Plan (RDD manipulations) on the cluster.
    
    * To execute code, we must write code. This code is then submitted to Spark either through the console or via a submitted job. This code then passes through the Catalyst Optimizer, which decides how the code should be executed and lays out a plan for doing so before, finally, the code is run and the result is returned to the user

    ![catalyst optimizer](/./images/catalyst_optimizer.PNG?raw=true "catalyst optimizer")
    
    * first phase of execution is meant to take user code and convert it into a logical plan
    
     ![local planning process](/./images/local_planning.PNG?raw=true "SparkTypes")
   
    * fter successfully creating an optimized logical plan, Spark then begins the physical planning process. The physical plan, often called a Spark plan, specifies how the logical plan will execute on the cluster by generating different physical execution strategies and comparing them through a cost model,

   ![physical planning process](/./images/physical_planning.PNG?raw=true "SparkTypes")


