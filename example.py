df = spark.read.csv("trainingData.csv").toDF("Category","id","date","flag","user","tweet")
