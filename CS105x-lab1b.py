# Final version of CS105x-lab1b
from pyspark.sql.functions import *

#lambda expression for filter age > 20
udf20 = udf(lambda a: a > 20, BooleanType())

#apply the lambda expression
dataDF.filter(udf20(dataDF.age))

#select columns, concatenate FullName
dataDF.select(concat(dataDF.first_name, lit(' '), dataDF.last_name).alias("FullName"), dataDF.occupation)

#Make Action Show
dataDF.show(truncate=False)