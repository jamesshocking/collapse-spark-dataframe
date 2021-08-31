# Working with Complex Data Types in Apache Spark Made Simple
## A Function to Collapse Complex Data Types into Individual Columns (Relational Format)

Apache Spark natively supports complex data types, and in some cases like JSON where an appropriate data source connector is available, it makes a pretty decent dataframe representation of the data.  Top level key value pairs are presented in their own columns, whilst more complex hierarchy data is persisted using column cast to a complex data type.  Using dot notation within the select clause, individual data points within this complex object can be selected.  For example:

```python
from pyspark.sql.functions import col

jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Jo","address":{"city":"Houston",' + \
      '"state":"Texas","zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
source_json_df = spark.read.json(otherPeopleRDD)

source_json_df.select(col("car.color"), col("car.model")).show()
```

This will return the following dataframe:

| color | model |
|-------|----|
| red   | jaguar |

This mechanism is simple and it works.  However, if the complexity of the data is multiple levels deep, spans a large number of attributes and/or columns, each aligned to a different schema and the consumer of the data isn't able to cope with complex data, the manual approach of writing out the Select statement can be labour intensive and be difficult to maintain (from a coding perspective).  One such data source is the Google Analytics dataset on Google Big Query ([https://support.google.com/analytics/answer/3437719?hl=en](https://support.google.com/analytics/answer/3437719?hl=en)).

To simplify working with complex data, the Python code in this repository is designed to transform multi-level complex hierarchical columns into a non-hierarchical verison of themselves.  Essentially, a dataframe that has no complex data type columns.  All nested attributes are assigned their own column named after their original location.  For example:

```text
car.color

becomes

car_color
```

### Getting Started, the Approach

Lets assume that we need to transform the following JSON, which has been loaded into Spark using spark.read.json:

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

Next we'll traverse the schema, creating a list of all available attributes, noting their entire ancestral path.  Our goal will be to create meta-data in the form of an array where each element is the complete ancestral branch of each value.  As complex data is hierarchical, a recursive function will be needed to walk all tree branches.  Finally we'll process the meta-data to create a collection of Column objects, using the dot-notation convention to select each attribute and then use the alias property to assign a unique name.  We'll use each attribute's path as the alias as described above.

### Parsing the Schema

Apache Spark schemas are a combination of StructType and StructField objects, with the StructType representing the top level object for each branches, including the root.  StructType owns a collection of StructFields accessed via the fields property.  Each StructField object is instantiated with three properties, name, data type and its nullability.  For example, if we run the following code against the dataframe created above:

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

Apache Spark supports a number of different data types including String and Integer for example, plus a StructType itself.  When a new branch is required, the StructField's data type is set to StructType as demonstrated in the example above.

A recursive function is one that calls itself and it is ideally suited to traversing a tree structure such as our schema.  For example:

```python
from pyspark.sql.types import StructType

def get_all_columns_from_schema(schema, depth=0):
  for field in schema.fields:
    field_name = ""
    for i in range(depth):
      field_name += "--"
      
    field_name += field.name
    print(field_name)
    if isinstance(field.dataType, StructType):    
      get_all_columns_from_schema(field.dataType, depth+1)   
      
#
get_all_columns_from_schema(source_json_df.schema)
```

Assuming that we're using the source_json_df dataframe declared above, if we execute this code against it, we will see the following output:

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

Recursion solves one problem but Python raises another.  Unfortunately Python does not support the passing of function attributes by reference.  When you pass a variable to a function, Python makes a copy of it, no reference to the original is maintained.  Every time we iterate get_all_columns_from_schema, Python makes a copy of the two parameters, schema and depth such that when we increment depth by 1, the original copy of depth remains unchanged and only the instance received by the next iteration of the function is updated.

This is a problem as each iteration of get_all_columns_from_schema will not know what came before it.  Whilst we would be able to create an array for each branch, we have no way of collating all branch arrays together into a list that can be returned to the executing code.  The code that will create our select statement.  To overcome this Python limitation, we need to wrap the parse function within another function (or a class but a function is more simple), and use the context of the parent function as a container for our meta-data array.

```python
from pyspark.sql.types import StructType

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

The main outer function, get_all_columns_from_schema, now takes the dataframe schema as a single input parameter.  The function starts by declaring a list, which is effectively global for the inner function.  This is the list that will persist all branches in their array form.  The recursive function is declared within get_all_columns_from_schema and is the same as the one demonstrated above, albeit with some minor tweaks (changing the depth counter with a list to persist all ancestor nodes for an individual branch).  In-addition, the call to print has been replaced with an append to the branches list owned by the outer function.

If we run this code against our dataframe's schema, get_all_columns_from_schema will return the following list:

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

### Collapsing Structured Columns

Now that we have the meta-data for all branches, the final step is to create an array that will hold the dataframe columns that we want to select, iterate over the meta-data list, and create Column objects initialised using the dot-notation address of each branch value before assigning a unique alias to each one.

```python
  from pyspark.sql.functions import col

  _columns_to_select = []
  _all_columns = get_all_columns_from_schema(source_json_df.schema)
  for column_collection in _all_columns:
    if len(column_collection) > 1:
      _columns_to_select.append(col('.'.join(column_collection)).alias('_'.join(column_collection)))
    else:
      _columns_to_select.append(col(column_collection[0]))
```

We start by initialising an array with the output from get_all_columns_from_schema, before iterating with a loop, and testing each element for its item length.  If the length is greater than one then it's a branch else it's the name of a regular non-hierarchical column.  Using the join method on a Pythons string, we concatenate the array members together, first to create the dot-notation string to select the branch value, and second to declare the new column's alias.  

The new array, _columns_to_select, now contains everything we need to completely collapse all complex data types, creating a column for each individual value.  Executing:

```python
collapsed_df = source_json_df.select(_columns_to_select)
collapsed_df.show()
```

Outputs the following dataframe:

|address_city|address_state|address_zip_first|address_zip_second|car_color|car_model|name|
|------------|-------------|-----------------|------------------|---------|---------|----|
|     Houston|        Texas|             1234|              4321|      red|   jaguar|  Jo|
