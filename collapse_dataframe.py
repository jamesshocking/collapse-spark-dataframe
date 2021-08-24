from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()

jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Yin","address":{"city":"Houston","state":"Ohio", "zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
df = spark.read.json(otherPeopleRDD)
  

def get_all_columns_from_schema(schema):  
  _master_list = []
  def inner_get(schema, ancestor=[]):
    for field in schema.fields:
      _current_path = ancestor+[field.name]
      if isinstance(field.dataType, StructType):    
        inner_get(field.dataType, _current_path)     
      else:
        _master_list.append(_current_path)
  
  #
  inner_get(schema)

  return _master_list

def collapse_columns(source_schema):
  _columns_to_select = []
  _all_columns = get_all_columns_from_schema(source_schema)
  for column_collection in _all_columns:
    if len(column_collection) > 1:
      _columns_to_select.append(col('.'.join(column_collection)).alias('_'.join(column_collection)))
    else:
      _columns_to_select.append(column_collection[0])

  return _columns_to_select

def collapse_to_dataframe(source_df):
  return source_df.select(collapse_columns(source_df.schema))
  
# now test
collapse_to_dataframe(df).show()

