# ch 3 a tour of sparks toolset

## Running Production Applications
    * Spark also makes it easy to turn your interactive exploration into production applications with spark-submit, a built-in command-line tool. spark-submit does one thing: it lets you send your application code to a cluster and launch it to execute there. 

## Datasets: Type-Safe Structured APIs
    * first API we’ll describe is a type-safe version of Spark’s structured API called Datasets, for writing statically typed code in Java and Scala. The Dataset API is not available in Python and R, because those languages are dynamically typed.
    * Dataset API gives users the ability to assign a Java/Scala class to the records within a DataFrame and manipulate it as a collection of typed objects, similar to a Java ArrayList or Scala Seq. The APIs available on Datasets are type-safe, meaning that you cannot accidentally view the objects in a Dataset as being of another class than the class you put in initially. This makes Datasets especially attractive for writing large applications, with which multiple software engineers must interact through well-defined interfaces.
    * One great thing about Datasets is that you can use them only when you need or want to. For instance, in the following example, we’ll define our own data type and manipulate it via arbitrary map and filter functions
    ```scala
        
        case class Flight(DEST_COUNTRY_NAME: String,
                            ORIGIN_COUNTRY_NAME: String,
                            count: BigInt)
        val flightsDF = spark.read  
            .parquet("/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightsDF.as[Flight]
    ```
    * when you call collect or take on a Dataset, it will collect objects of the proper type in your Dataset, not DataFrame Row

    ```scala
        
    flights  
        .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
        .map(flight_row => flight_row)  
        .take(5)
        
    flights  
        .take(5)  
        .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")  
        .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
    ```

## Structured Streaming
    * Structured Streaming is a high-level API for stream processing that became production-ready in Spark 2.2
    * basically you can take the  same operations you preform in batch mode using sparks structured API's and run them in a streaming fashion
    * best thing about Structured Streaming is that it allows you to rapidly and quickly extract value out of streaming systems with virtually no code changes.
    * first analyze the data as a static dataset and create a DataFrame to do so. We’ll also create a schema from this static dataset

    ```scala
        val staticDataFrame = spark.read.format("csv")  
            .option("header", "true")  
            .option("inferSchema", "true")  
            .load("/data/retail-data/by-day/*.csv")
        
        staticDataFrame.createOrReplaceTempView("retail_data")
        val staticSchema = staticDataFrame.schema
    ```

    ```python
        staticDataFrame = spark.read.format("csv")\  
            .option("header", "true")\  
            .option("inferSchema", "true")\  
            .load("/data/retail-data/by-day/*.csv")
        
        staticDataFrame.createOrReplaceTempView("retail_data")
        staticSchema = staticDataFrame.schema
    ```

    * take a look at the sale hours during which a given customer (identified by CustomerId) makes a large purchase. For example, let’s add a total cost column and see on what days a customer spent the most.The window function will include all data from each day in the aggregation. It’s simply a window over the time–series column in our data. This is a helpful tool for manipulating date and timestamps because we can specify our requirements in a more human form (via intervals), and Spark will group all of them together for us:

    ```python
        
        from pyspark.sql.functions import window, column, desc, col
        staticDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
            .groupBy(col(
                "CustomerId"), window(col("InvoiceDate"), "1 day"))
            .sum("total_cost").show(5)
    ```
    
    * take a look at streaming code:
    * very similar to reading static data only change is using readStream instead of read
    * see below for stream and above for static
    * additionally you’ll notice the maxFilesPerTrigger option, which simply specifies the number of files we should read in at once. This is to make our demonstration more “streaming,” and in a production scenario this would probably be omitted.

    ```python
        streamingDataFrame = spark.readStream.schema(staticSchema)\   
            .option("maxFilesPerTrigger", 1).format("csv").option("header", "true")\
            .load("/data/retail-data/by-day/*.csv")
    ```

    * to see wheater our DataFrame is streaming
    ```python
        streamingDataFrame.isStreaming # returns true id readStream aka streaming
    ```
    
    * putting together streaming and above summation in theprocess:
    ```python
        purchaseByCustomerPerHour = streamingDataFrame\
            .selectExpr( 
                "CustomerId",
                "( UnitPrice * Quantity ) as total_cost",
                "InvoiceDate")\ 
            .grouBy(
                col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
            .sum("total_cost")
    ```
    
    * This is still a lazy operation, so we will need to call a streaming action to start the execution of this data flow.

    * Streaming actions are a bit different from our conventional static action because we’re going to be populating data somewhere instead of just calling something like count (which doesn’t make any sense on a stream anyways). The action we will use will output to an in-memory table that we will update after each trigger. In this case, each trigger is based on an individual file (the read option that we set). Spark will mutate the data in the in-memory table such that we will always have the highest value as specified in our previous aggregation:

    ```python
        purchasByCustomerPerHour.writeStream\
            .format("memory")\
            .queryName("customer_purchases")\
            .outputMode("complete")\
            .start()
    ```
    
    * when we start stream we can run queries against it to debug what our results will look like 

    * ex
    ```python
        spark.sql("""
            SELECT *
            FROM customer_purchases
            ORDER BY `sum(total_cost)` DESC
        """)\
        .show(5)
    ```
    
    * composition of tables changes may change with each file read 
    * to write results out to console 
    ```python
        .format("console") # instead of "memory"
    ```

## Machine Learning and advanced analytics

    * benifit of spark is aboility to preform large-scale machine learning with built-in libary of machine learning algos called MLli
    *from classification to regression, and clustering to deeep learning

    * example to demonstrate this functionality we will use k-means

        * 𝘬-means is a clustering algorithm in which “𝘬” centers are randomly assigned within the data. The points closest to that point are then “assigned” to a class and the center of the assigned points is computed. This center point is called the centroid. We then label the points closest to that centroid, to the centroid’s class, and shift the centroid to the new center of that cluster of points. We repeat this process for a finite set of iterations or until convergence (our center points stop changing).

    * our current data is represented by variety types but MLlib require that data is represented as numerical values. Therefor to use k-means or any machine learning algo in MLlib we need to use transformations to manipulate our date data:

    ```python
        from pyspark.sql.functions import date_format, col

        preppedDataFrame = staticDataFrame.na.fill(0)\
            withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
            .coalesce(5)
    ```
    
    * spliting the data into training and test sets
    ```python
        trainDataFrame = preppedDataFrame\
            .where("InvoiceDate < '2011-07-01'")
        testDataFrame = preppedDataFrame\
            .where("InvoiceDate >= '2011-07-01'")
    ```
    
    * Sparks MLlib also provides a number of transformations with which we can automate some of our general transformations ex StringIndexer
    * day of week to index ex saturday = 6
    ```python
        from pyspark.ml.feature import StringIndexer 
        indexer = StringIndexer()\
            .setInputCol("day_of_week")\
            .setOutputCol("day_of_week_index")
    ```
    
    * change index to col of options with the one selected

    ```python
        from pyspak.ml.feature import OneHotEncoder
        encoder = OneHotEncoder()\
            .setInputCol("day_of_week_index")\
            .setOutputCol("day_of_week_encoded")
    ```
   
    * to use Machine learning algos in Spark take a vector type(must be a set of numerical values)
    
    ```python
        from pyspark.ml.feature import VectorAssembler
        
        vectorAssembler = VectorAssembler()\
            .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
            .setOutputCol("features")
    ```

    * above they have 3 key features and we added it to new col called features    
    * below we set it up into a pipeline
    ```python
        from pyspark.ml import Pipeline

        transformationPipeline = Pipeline()\
            .setStages([indexer, encoder, vectorAssembler])
    ```
    
    * below we fit our transformaer to the dataset
    ```python
        fittedPipeline = transformationPipeline.fit(trainDataFrame)
    ```

    * take that fittedpipeline and use it to transorm all of data ina consistent and repeatable way:
    
    ```python
        transformedTraining = fittedPipeline.transform(trainDataFrame)
    ```

    * This will put a copy of the intermediately transformed dataset into memory, allowing us to repeatedly access it at much lower cost than running the entire pipeline again

    ```python
        transformedTraing.cache()
    ```

    * now that we have trainning set its time to train the model
    * first we well have to import relevant model 
    * we will use kmeans like stated above

    ```python
        from pyspark.ml.clustering import KMeans
        kmeans = KMeans().setK(20).setSeed(1L)
    ```
    
    * In Spark, training machine learning models is a two-phase process. First, we initialize an untrained model, and then we train it. There are always two types for every algorithm in MLlib’s DataFrame API. They follow the naming pattern of Algorithm, for the untrained version, and AlgorithmModel for the trained version. In our example, this is KMeans and then KMeansModel.

    ```python
        kmModel = kmeans.fit(transformedTraining)
    ```

    * After we train this model, we can compute the cost according to some success merits on our training set
    ```python
        print kmModel.computeCost(transformedTraining)
        transformedTest = fittedPipeline.transform(testDataFrame)
        print kmModel.computeCost(transformedTest)
    ```

## Lower-level apis
    * Spark includes a number of lower-level primitives to allow for arbitrary Java and Python object manipulation via Resilient Distributed Datasets (RDDs)

    * There are some things that you might use RDDs for, especially when you’re reading or manipulating raw data, but for the most part you should stick to the Structured APIs. RDDs are lower level than DataFrames because they reveal physical execution characteristics (like partitions) to end users.

    * One thing that you might use RDDs for is to parallelize raw data that you have stored in memory on the driver machine. For instance, let’s parallelize some simple numbers and create a DataFrame after we do so. We then can convert that to a DataFrame to use it with other DataFrames:
    
    ```python
        from pyspark.sql import Row
        spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()
    ```

    * RDDs are available in Scala as well as Python. However, they’re not equivalent. This differs from the DataFrame API (where the execution characteristics are the same) due to some underlying implementation details. We cover lower-level APIs, including RDDs in Part IV. As end users, you shouldn’t need to use RDDs much in order to perform many tasks unless you’re maintaining older Spark code. There are basically no instances in modern Spark, for which you should be using RDDs instead of the structured APIs beyond manipulating some very raw unprocessed and unstructured data.

## SparkR

    * a tool for running R on Spark
    * follows the same principals as all of Sparks other language bindings
    * very similar to python api except that it follows R's syntax instead of Python

    ```r
        library(SparkR)
        sparkDF <- read.df("/data/flight-data/csv/2015-summary.csv",
            source = "csv", header="true", inferSchema = "true")
        take(sparkDF, 5)


        collect(orderBy(sparkDF, "count"), 20)
    ```

    * R users can also use other R libraries like the pipe operator in magrittr to make Spark transformations a bit more R-like. This can make it easy to use with other libraries like ggplot for more sophisticated plotting:

    ```r
        library(magrittr)
        sparkDF %>%  
            orderBy(desc(sparkDF$count)) %>%  
            groupBy("ORIGIN_COUNTRY_NAME") %>%  
            count() %>%  
            limit(10) %>%  
            collect()
    ```

## Spark Ecosystem and packages
    * You can find the largest index of Spark Packages at spark-packages.org, where any user can publish to this package repository. There are also various other projects and packages that you can find on the web; for example, on GitHub.

