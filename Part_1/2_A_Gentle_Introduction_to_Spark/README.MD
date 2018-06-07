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
    ``` python 
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

    





