# Simplify working with Structured Data in Apache Spark
## A Function to Collapse a Structured Data Type into Individual Columns

Apache Spark natively supports hierarchical data.  Using dot notation within the select clause, individual data points can be selected from a structured column's nested values.  For example:

```python
jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Jo","address":{"city":"Houston","state":"Texas", "zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
source_json_df = spark.read.json(otherPeopleRDD)

source_json_df.select(col("car.color"), col("car.model").show()
```

This will return the following table frame:

| color | model |
|-------|----|
| red   | jaguar |

This mechanism is great, reliable and it works.  However, if the structure of the data is multiple levels deep, spans a large number of columns, each aligned to a different schema and the consumer of the data isn't able to cope with hierarchical data, this manual approach can be labour intensive and lead to difficult to maintain code.  One such data source is the Google Analytics dataset on Google Big Query ([](https://support.google.com/analytics/answer/3437719?hl=en)).

To simplify working with and using structured data, the Python code in this repository is designed to transform multi-level structured hierarchical columns into a non-hierarchical verison of themselves.  Essentially, a dataframe that has no hierarchical columns and all values are persisted within their own column.  All nested attributes are assigned their own column labelled after their original location, thereby preserving a label that describes what the value represents.  For example:

```text
car.color

becomes

car_color
```

### Getting Started, the Approach

Continuing with the JSON example above, we have the following JSON persisted within a single Dataframe row and column:

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

The first task to perform is to create a function that can parse the schema bound to the Dataframe.  The schema is accessed via a property of the same name found on the dataframe itself.  

Next we need to traverse the schema identifying all the available attributes that we will select as columns.  A recursive function will be required for this and our goal will be to create meta-data in the form of an array, which describes all available attributes.  Finally we'll process this meta data to create a collection of Column objects, using the dot-notation convention to select each attribute and the alias property of a Column to create a unique name.  We'll use each attribute's path as described above.

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

Recursion solves one problem but Python raises another.  Unfortunately Python does not support passing function attributes by reference.  What this means is that when you pass a value to a function, Python makes a copy of it.  In the case of the recursion example, for each iteration of the parse_schema function, Python makes a copy of the depth parameter rather than modify the original version.

This is a problem as each iteration of the parse_schema function will not know what came before it.  Without somethinge extra, we will not be able to create an array of branches and return them as the output of the parse function.
