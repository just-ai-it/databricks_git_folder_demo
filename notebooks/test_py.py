from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row

# Create dummy data
data = [
    Row(label=1.0, features=Vectors.dense(0.0)),
    Row(label=2.0, features=Vectors.dense(1.0)),
    Row(label=3.0, features=Vectors.dense(2.0)),
    Row(label=4.0, features=Vectors.dense(3.0))
]

df = spark.createDataFrame(data)

# Create a Linear Regression model
lr = LinearRegression(featuresCol='features', labelCol='label')

# Fit the model
lr_model = lr.fit(df)

# Display the coefficients and intercept
coefficients = lr_model.coefficients
intercept = lr_model.intercept

result = spark.createDataFrame([(coefficients[0], intercept)], ["Coefficient", "Intercept"])
display(result)