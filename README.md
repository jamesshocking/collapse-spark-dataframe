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

Next we need to traverse the schema identifying all available attributes noting their entire ancestral inheritence.  Our goal will be to create meta-data in the form of an array per attribute (identifying their entire branch), and then collating all these arrays together into a single list.  As the data is hierarchical, a recursive function is needed to walk all tree branches.  Finally we'll process the returned meta-data to create a collection of Column objects, using the dot-notation convention to select each attribute, albeit as parameter to be passed to the Column class and then use the alias property to assign a unique name.  We'll use each attribute's path as the alias as described above.

### Parsing the Schema

Apache Spark schemas are a combination of StructType and StructField objects, with the StructType being the top level object of all branches, including the root.  It owns a collection of StructFields accessed via the fields property.  Each StructField object is instantiated with three properties, name, data type and its nullability.  For example, if we run the following code against the dataframe created above:

```python
schema = source_json_df.schema
print(schema)
```

The output would be:

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

Apache Spark supports a number of different data types including String and Integer for example, plus a StructType itself.  When a new branch is required, the StructType is used as the data type as demonstrated in the example above.

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

If you execute this code on the source_json_df dataframe declared above, you will see the following output:

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

Recursion solves one problem but Python raises another.  Unfortunately Python does not support passing function attributes by reference.  When you pass a value to a function, Python makes a copy of it, no reference to the original is maintained.  Every time we iterate parse_schema, Python makes a copy of the two parameters, schema and depth such that when we increment depth by 1, the original copy of depth remains unchanged and only the instance received by the function is updated.

This is a problem as each iteration of parse_schema will not know what came before it.  Whilst we could be able to create an array for each branch, we have no way of collating all branch arrays together into a list that can be returned to the executing code.  To overcome this limitation, we need to wrap the parse function within another function (or a class but a function is more simple), and use the context of the parent function as a place holder for all hierarchical pathways that represent the complete schema.

```python
def get_all_columns_from_schema(source_schema):
  branches = []
  def inner_get(schema, ancestor=[]):
    for field in schema.fields:
      branch_path = ancestor+[field.name]     
      if isinstance(field.dataType, StructType):    
        inner_get(field.dataType, branch_path) 
      else:
        branches.append(branch_path)
        
  inner_get(source_schema)
        
  return branches
```

Them main outer function "get_all_columns_from_schema" expects the dataframe schema as a single input parameter.  The function starts by declaring a list, which is effectively global for the inner function.  This is the list that collects all branches in their array form.  The recursive function is declared within "get_all_columns_from_schema" and is the same as the demonstration above, albeit with minor tweaks changing the depth counter with a list to persist all ancestor nodes for an individual branch.  In-addition, the call to print has been replaced with an append to the branches list owned by the outer function.

If we run this code against our dataframe's schema, "get_all_columns_from_schema" will return the following list:

```python
[
  ['address', 'city'], 
  ['address', 'state'], 
  ['address', 'zip', 'first'], 
  ['address', 'zip', 'second'], 
  ['car', 'color'], 
  ['car', 'model'], 
  ['name']
]
```

### Collapsing the Structured Columns
