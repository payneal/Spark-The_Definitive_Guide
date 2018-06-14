# ch 3 a tour of sparks toolset

* Running Production Applications
    * Spark also makes it easy to turn your interactive exploration into production applications with spark-submit, a built-in command-line tool. spark-submit does one thing: it lets you send your application code to a cluster and launch it to execute there. 

* Datasets: Type-Safe Structured APIs
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

* Structured Streaming
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





