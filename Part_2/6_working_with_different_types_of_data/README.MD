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

