# chapter 6 working with different types of Data
* chapter covers expressions, which are the bread and  butter of Sparks structured operations 
* also reviewing the following
    * Booleans
    * Numbers
    * Strings
    * Dates and timestamps
    * handling null
    * Complex types
    * User-defined functions


## converting  to Spark Types
* convert native types to Spark types can be done with lit function
    * lit() this function converts a type in another language to its corresponding Spark representation 
    * below how we convert a couple of different kinds in Python
    ```python
        
        from pyspark.sql.functions import lit
        df.select(lit(5), lit("five"), lit(5.0))
    
    ```

## Working with Booleans
* booleans are essential when it comes to data analysis because they are the foundation for all filtering
* boolean statements consists of four  elements:
    * and
    * or
    * true
    * false
* 
        
