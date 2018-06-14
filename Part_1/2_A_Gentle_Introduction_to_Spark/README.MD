# ch 2.  A gental Introduction to Spark
* One particularly challenging area is data processing. Single machines do not have enough power and resources to perform computations on huge amounts of information (or the user probably does not have the time to wait for the computation to finish). A cluster, or group, of computers, pools the resources of many machines together, giving us the ability to use all the cumulative resources as if they were a single computer. Now, a group of machines alone is not powerful, you need a framework to coordinate work across them. Spark does just that, managing and coordinating the execution of tasks on data across a cluster of computers.

* Spark Applications
    * onsist of a driver process and a set of executor processes. The driver process runs your main() function, sits on a node in the cluster, and is responsible for three things: maintaining information about the Spark Application; responding to a user’s program or input; and analyzing, distributing, and scheduling work across the executors (discussed momentarily)
    * executors are responsible for actually carrying out  the work that the driver assigns them. This means that each executor is responsible for only two things: executing code assigned to it by the driver, and reporting the state of the computation on that executor back to the driver node.
    * key points
        * Spark employs a cluster manager that keeps track of the resources available.
        * The driver process is responsible for executing the driver program’s commands across the executors to complete a given task.

* Spark Language APIs
    * Scala =  primarily written in Scala, making it Spark’s “default” language
    * Java = Even though Spark is written in Scala, Spark’s authors have been careful to ensure that you can write Spark code in Java
    * Python = supports nearly all constructs that Scala supports
    * sql = Spark supports a subset of the ANSI SQL 2003 standard. This makes it easy for analysts and non-programmers to take advantage of the big data powers of Spark
    * r = Spark has two commonly used R libraries: one as a part of Spark core (SparkR) and another as an R community-driven package (sparklyr).

* Each language API maintains the same core concepts 
* Although you can drive Spark from a variety of languages, what it makes available in those languages is worth mentioning. Spark has two fundamental sets of APIs: the low-level “unstructured” APIs, and the higher-level structured APIs.

* The SparkSession
    * you control your Spark Application through a driver process called the SparkSession. The SparkSession instance is the way Spark executes user-defined manipulations across the cluster. There is a one-to-one correspondence between a SparkSession and a Spark Application.

    * perform the simple task of creating a range of numbers. This range of numbers is just like a named column in a spreadsheet:
        * scala
    ```scala
        
        val myRange = spark.range(1000).toDF('number')
    
    ```
        * python
    ```python 
    
        myRange = spark.range(1000).toDF("number")
    
    ```
    * created a DataFrame with one column containing 1,000 rows with values from 0 to 999.
    * DataFrame is the most common Structured API and simply represents a table of data with rows and columns. The list that defines the columns and the types within those columns is called the schema. You can think of a DataFrame as a spreadsheet with named columns
    * When run on a cluster, each part of this range of numbers exists on a different executor.

* DataFrames
    * A DataFrame is the most common Structured API and simply represents a table of data with rows and columns. The list that defines the columns and the types within those columns is called the schema. You can think of a DataFrame as a spreadsheet with named columns. Figure 2-3 illustrates the fundamental difference: a spreadsheet sits on one computer in one specific location, whereas a Spark DataFrame can span thousands of computers. The reason for putting the data on more than one computer should be intuitive: either the data is too large to fit on one machine or it would simply take too long to perform that computation on one machine.
    * ataFrame concept is not unique to Spark. R and Python both have similar concepts. However, Python/R DataFrames (with some exceptions) exist on one machine rather than multiple machines. This limits what you can do with a given DataFrame to the resources that exist on that specific machine. However, because Spark has language interfaces for both Python and R, it’s quite easy to convert Pandas (Python) DataFrames to Spark DataFrames, and R DataFrames to Spark DataFrames.

* Transformations
    * core data structures are immutable, meaning they cant be changed after they are created
    * to change a dataframe you need to instruct  Spark how you would like to modify it to do what you want. This in the Spark would is considered a "TRANSFORMATION"
    * simple transformation
    * scala
    ```scala
        val divisBy2 = myRange.where("number % 2 = 0")

    ```
        * python
    ``` python 
        divisBy2 = myRange.where("number % 2 = 0")

    ```

    * 2 type of transformations: thoes that  specify narrow dependiencies, and thoes that specify wide dependencies 
 
        * narrow dependencies
            * are those for which each input partition will contribute to only one output partition

        * wide dependencies
            * style transformation will have input partitions contributing to many output partitions. You will often hear this referred to as a shuffle whereby Spark will exchange partitions across the cluster. With narrow transformations, Spark will automatically perform an operation called pipelining, meaning that if we specify multiple filters on DataFrames, they’ll all be performed in-memory. The same cannot be said for shuffles. When we perform a shuffle
    
    * transformations are simply ways of specifying different series of data manipulation. This leads us to a topic called lazy evaluation.

* Lazy evaluation
    * Lazy evaulation means that Spark will wait until the very last moment to execute the graph of computation instructions

    * In Spark, instead of modifying the data immediately when you express some operation, you build up a plan of transformations that you would like to apply to your source data. By waiting until the last minute to execute the code, Spark compiles this plan from your raw DataFrame transformations to a streamlined physical plan that will run as efficiently as possible across the cluster
    * provides immense benefits because Spark can optimize the entire data flow from end to end

    * an example of this is something called predicate pushdown on DataFrames. If we build a large Spark job but specify a filter at the end that only requires us to fetch one row from our source data, the most efficient way to execute this is to access the single record that we need. Spark will actually optimize this for us by pushing the filter down automatically.

* Actions
    * transformations allow us to build up our logical transformation plan. 
    * to trigger the coputation, we  run an action. and an action  instructs Spark to computate a result from a series of transformations. The simmplist action is  cout
    ```python
        divisBy2.count()
    ```
    
    * the output should be 500 continuing with our example
    * 3 kinds of actions:
        * actions to view data 
        * actions to collect data to native objects in the respective langague
        *  actions to write to output data sources

* End to End Example
    * sort does not modify the DataFrame. We use sort as a transformation that returns a new DataFrame by transforming the previous DataFrame

    *  python
    ```python
        
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        
        conf = SparkConf()
        spark = SparkSession.builder.config().getOrCreate()

        flightData2015 = spark.read.option("inferSchema", "true")
            .option("header", "true")\
            .csv("./data/2015-summary.csv")

        flightData2015.sort('count').explain()
    
        # example of info
        
      	# == Physical Plan == 
		# *Sort [count#195 ASC NULLS FIRST], true, 0
		# +- Exchange rangepartitioning(count#195 ASC NULLS FIRST, 200)   
		# 	+- *FileScan csv [DEST_COUNTRY_NAME#193,ORIGIN_COUNTRY_NAME#194,count#195] ...  

    ```
	
	* You can read explain plans from top to bottom, the top being the end result, and the bottom being the source(s) of data. In this case, take a look at the first keywords. You will see sort, exchange, and FileScan. That’s because the sort of our data is actually a wide transformation because rows will need to be compared with one another. Don’t worry too much about understanding everything about explain plans at this point, they can just be helpful tools for debugging and improving your knowledge as you progress with Spark.

    * By default, when we perform a shuffle, Spark outputs 200 shuffle partitions. Let’s set this value to 5 to reduce the number of the output partitions from the shuffle:
    * how to set conf settings on spark
    ```python
    
        conf = SparkConf() 
        conf.set("spark.sql.shuffle.partitions", "5")

    ```
    *  you can change this to help control the physical execution characteristics of your Spark jobs. Go ahead and experiment with different values and see the number of partitions yourself. In experimenting with different values, you should see drastically different runtimes.
	
* DataFrames and SQL
    
    * make any dataframe into a table or view with one simple method call:
    ```python
        flightData2015.createOrReplaceTempView("flight_data_2015")
    ```
    * once you do that spark.sql("select * from flight_data_2015") will work
    
    * discussed: groupBy, sum, sort, renameColumn, limit
        * groupBy =  we’re going to be grouping by a key (or set of keys) and that now we’re going to perform an aggregation over each one of those keys.
        * sum aggregation method. This takes as input a column expression or, simply, a column name. The result of the sum method call is a new DataFrame. You’ll see that it has a new schema but that it does know the type of each column
        * withColumnRenamed method that takes two arguments, the original column name and the new column name. Of course, this doesn’t perform computation: this is just another transformation!
        * limit statement as well as the orderBy (in the first line). You can also see how our aggregation happens in two phases, in the partial_sum calls. This is because summing a list of numbers is commutative, and Spark can perform the sum, partition by partition. Of course we can see how we read in the DataFrame
