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

*  boolean expressions are not just reserved to filters you can also just dpecify a Boolean column

* it’s often easier to just express filters as SQL statements than using the programmatic DataFrame interface and Spark SQL allows us to do this without paying any performance penalty. For example, the following two statements are equivalent:

```python
	df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\  
		.where("isExpensive")\  
		.select("Description", "UnitPrice").show(5)
```

* warning
	* One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions. If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure that you perform a null-safe equivalence test:
	
	```python
		df.where(col("Description").eqNullSafe("hello")).show()
	```
# working with numbers
* When working with big data, the second most common task you will do after filtering things is counting things. For the most part, we simply need to express our computation, and that should be valid assuming that we’re working with numerical data types.To fabricate a contrived example, let’s imagine that we found out that we mis-recorded the quantity in our retail dataset and the true quantity is equal to (the current quantity * the unit price)2 + 5. This will introduce our first numerical function as well as the pow function that raises a column to the expressed power

```python
	
	from pyspark.sql.functions import expr, pow
	fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
	df.select(
		expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
```

* Naturally we can add and subtract as necessary, as well. In fact, we can do all of this as a SQL expression

```python
    df.selectExpr(
        "CustomerId",
        "(POWER((QUantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
```

* another common task is  rounding
```python
    from pyspark.sql.functions impoort lit, round, bround

    df.select(round(2.5), bround(2.5).show()
```

* compute the correlation of two columns
``` python

    from pyspark.sql.functions import corr
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()

```

* compute summary statitics for  a column or set of columns
    * takes a;; nmeric columns and calculate the count, mean, standard deviation, min and max

```python
    df.describe().show()
```

* if you wish to  calculate these  number yourself then:
```python
    
    colName = "UnitPrice"
    quantileProbs = [0.5]
    relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51
```

* to see a cross-tabulation or freequesnt itempairs
```python
    
    df.stat.crosstab("StockCode", "Quantity").show()
    # or
    df.stat.crosstab(["StockCode", "Quantity"]).show()
```

* need to add and id to everything ... easy
```python
    from pyspark.sql.functions import monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(2)
```

# working with strings
* string manipulation is common so its worth while knowing what you can do
    
    * captilize everything
    ```python
        from pyspark.sql.functions import initcap
        df.select(initcap(col("Description"))).show()
        
    ```

    * lower  case things
    ```python
        
        from pyspark.sql.functions import lower, upper
        df.select(
            col("Description"),
            lower(col("Description")),
            upper(lower(col("Description")))).show(2)
    ```
    
    *  adding or removing spaces around a string
    ```python
    
        from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
        df.select(    
            ltrim(lit("    HELLO    ")).alias("ltrim"),    
            rtrim(lit("    HELLO    ")).alias("rtrim"),    
            trim(lit("    HELLO    ")).alias("trim"),    
            lpad(lit("HELLO"), 3, " ").alias("lp"),    
            rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)
    ```
    * Note that if lpad or rpad takes a number less than the length of the string, it will always remove values from the right side of the string.
    
    * regular expressions
        * most frequentlkly used 
        * Spark takes advantage of the complete power of Java regular expressions. The Java regular expression syntax departs slightly from other programming languages, so it is worth reviewing before putting anything into production

    ```python
        from pyspark.sql.functions import regexp_replace
        regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
        df.select(regexp_replace(
            col("Description"),regex_string, "COLOR").alias(
                "color_clean"),col("Description")).show(2)
    ```

    * replace a given char with another one
    ```python
        df.select(translate(
            col("Description"), "LEFT", '1337'), col("description")).show(2)
        
    ```
    
    * regexp_extract
    ```python
    ```




