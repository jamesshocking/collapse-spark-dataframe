from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

# declare dummy data to demonstrate how the collapse mechanism works
jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Jo","address":{"city":"Houston","state":"Texas", "zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
df = spark.read.json(otherPeopleRDD)
  
# Recursively iterates over the schema, creating an array of arrays, whereby each item
# of the master array, is an array of column names
#
# For example, lets say there are three columns of which two are hierarchical and the following schema/structure
#    name
#    address
#      street
#      town
#    details
#      age
#      gender
#
# The function will return the following array:
# [["name"],["address","street"],["address","town"],["details","age"],["details","gender"]]
def get_all_columns_from_schema(schema):  
  _master_list = []
  def inner_get(schema, ancestor=[]):
    for field in schema.fields:
      _current_path = ancestor+[field.name]
      if isinstance(field.dataType, StructType):    
        inner_get(field.dataType, _current_path)     
      else:
        _master_list.append(_current_path)
  
  # kick off the recursive walk of all columns and any structures
  inner_get(schema)

  return _master_list

# collapse_columns is passed the dataframe schema, which is then passes
# to get_all_columns_from_schema.  On return, it iterates through the array
# of columns in order to build up the select list that will be used
# to collapse the hierarchical columns into a single 2d structure
#
# for example, lets say _all_columns has the following array: [["name"],["address","street"]]
# after iterating through the array, the function response will be
# [col("name"), col("address.street").alias("address_street")]
def collapse_columns(source_schema, columnFilter=""):
  _columns_to_select = []
  _all_columns = get_all_columns_from_schema(source_schema)
  for column_collection in _all_columns:
    if (len(columnFilter) > 0) & (column_collection[0] != columnFilter): 
        continue

    if len(column_collection) > 1:
      _columns_to_select.append(col('.'.join(column_collection)).alias('_'.join(column_collection)))
    else:
      _columns_to_select.append(col(column_collection[0]))

  return _columns_to_select

# as above but for individual columns
def collapse_column(source_df, source_column):
    column_name = ""
    if isinstance(source_column, Column):
      column_name = source_column.name
    else:
      column_name = source_column

    return collapse_columns(source_df.schema, column_name)

# returns a dataframe that has been collapsed.  Input is the dataframe to be collapsed
def collapse_to_dataframe(source_df):
  return source_df.select(collapse_columns(source_df.schema))
  
# now test
collapse_to_dataframe(df).show()

