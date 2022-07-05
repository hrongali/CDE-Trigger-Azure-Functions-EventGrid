import random
from pyspark.sql import SparkSession


def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

NUM_SAMPLES = 10000

count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()

print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
