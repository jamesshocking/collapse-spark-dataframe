# Simplify Working with Structured Data in Apache Spark
## A Function to Collapse a Structured Data Type into Individual Columns

Apache Spark natively supports hierarchical data.  Using dot notation within the select clause, individual data points can be selected from the nested values of a structured column.  For example:

```python
jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Jo","address":{"city":"Houston",' + \
      '"state":"Texas","zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
source_json_df = spark.read.json(otherPeopleRDD)

source_json_df.select(col("car.color"), col("car.model")).show()
```

This will return the following table frame:

| color | model |
|-------|----|
| red   | jaguar |

This mechanism is simple and it works.  However, if the structure of the data is multiple levels deep, spans a large number of columns, each aligned to a different schema and the consumer of the data isn't able to cope with hierarchical data, the manual approach of writing out the Select statement can be labour intensive and be difficult to maintain (from a coding perspective).  One such data source is the Google Analytics dataset on Google Big Query ([](https://support.google.com/analytics/answer/3437719?hl=en)).

To simplify working with structured data, the Python code in this repository is designed to transform multi-level structured hierarchical columns into a non-hierarchical verison of themselves.  Essentially, a dataframe that has no hierarchical columns and all values are persisted within their own column.  All nested attributes are assigned their own column named after their original location, thereby preserving a label that describes what the value is and where it came from.  For example:

```text
car.color

would become

car_color
```

### Getting Started, the Approach

Lets assume that we need to transform the following JSON, which is persisted within a single Dataframe row and column:

```json
{
  "car":{
    "color":"red", 
    "model":"jaguar"
  },
  "name":"Jo",
  "address":{
    "city":"Houston",
    "state":"Texas",
    "zip":{
      "first":1234,
      "second":4321
    }
  }
}
```

The first task is to create a function that can parse the schema bound to the Dataframe.  The schema is accessed via a property of the same name found on the dataframe itself.  

Next we need to traverse the schema identifying all the available attributes that we will select as columns.  Our goal will be to create meta-data in the form of an array, which describes all available attributes.  As the data is hierarchical, a recursive function is needed to walk all tree branches.  Finally we'll process the returned meta data to create a collection of Column objects, using the dot-notation convention to select each attribute and the Column alias property to assign a unique name.  We'll use each attribute's path as the alias.

### Parsing the Schema

Apache Spark organises schemas via a combination of StructType and StructField objects, with the StructType being the top level object.  It owns a collection of StructFields accessed via the fields property on the StructType object.  Each StructField object has three properties, the name, data type and its nullability.

```python
schema = source_json_df.schema
print(schema)
```

For the our test JSON, the print statement would output:

```python
StructType([
  StructField("car", StructType(
    StructField("color", StringType(), True),
    StructField("model", StringType(), True)
  ), True),
  StructField("name", StringType(), True),
  StructField("address", StructType(
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StructType(
      StructField("first", IntegerType(), True),
      StructField("second", IntegerType(), True)
    ), True)
  ), True)
])
```

The schema is a StructType, which has a collection of StructField objects.  Apache Spark supports a number of different data types including String and Integer for example, plus a StructType itself.  When a new branch is required, the StructType is used as the data type as demonstrated in the example above.

A recursive function is one that calls itself and it is ideally suited to traversing a tree structure such as our schema.  For example:

```python
def parse_schema(schema, depth=0):
  for field in schema.fields:
    field_name = ""
    for i in range(depth):
      field_name += "--"
      
    field_name += field.name
    print(field_name)
    if isinstance(field.dataType, StructType):    
      parse_schema(field.dataType, depth+1)   
      
#
parse_schema(source_json_df.schema)
    
```

If you execute this code using the source_json_df declared above, you will see the following output:

```text
address
--city
--state
--zip
----first
----second
car
--color
--model
name
```

Recursion solves one problem but Python raises another.  Unfortunately Python does not support passing function attributes by reference.  When you pass a value to a function, Python makes a copy of it, no reference to the original is maintained.  Every time we iterate parse_schema, Python makes a copy of the two parameters, schema and depth such that when we increment depth by 1, on the recursive function call, the original copy of depth remains unchanged.

This is a problem as each iteration of parse_schema will not know what came before it.  We will not be able to create an array of branches and return them as the output of the parse function.  To overcome this limitation, we wrap the parse function within another function, and use the context of the parent function as a place holder for all hierarchical pathways that represent the complete schema.
