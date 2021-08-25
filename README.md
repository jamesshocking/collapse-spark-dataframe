# Collapse Dataframe for Apache Spark

Apache Spark natively supports hierarchical data.  Using dot notation within the select clause, one can pull out individual data points from a structured column's nested values.  For example:

```python
jsonStrings = ['{"car":{"color":"red", "model":"jaguar"},"name":"Jo","address":{"city":"Houston","state":"Texas", "zip":{"first":1234,"second":4321}}}']
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
df = spark.read.json(otherPeopleRDD)

df.select(col("car.color"), col("car.model").show()
```

This will return the following table frame:

| color | model |
|-------|----|
| red   | jaguar |

This mechanism is great, reliable and it works.  However, if the structure of the data is multiple levels deep, spans a large number of columns, some hierarchical and the consumer of the data isn't able to cope with hierarchical data, this manual approach can be labour intensive and difficult to maintain.  One such data source is the Google Analytics dataset on Google Big Query ([](https://support.google.com/analytics/answer/3437719?hl=en)).

To simplify working with structured data and to support other use-cases such as consumption from a business intelligence tool (which usually do not support hierarchical columns), the Python code in this repository is designed to transform all multi-level structured hierarchical columns into a non-hierarchical verison of themselves.  Essentially, a dataframe that is not hierarchical.  All nested values are assigned their own column labelled after their original location, thereby preserving a label that describes what the value represents.

## Getting Started

Continuing with the JSON example above:

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

