from pyspark.ml import PipelineModel
from pyspark.mllib.evaluation import MulticlassMetrics

def realtime_predict(sc,spark):
    sc = sc
    spark = spark
    nb_path = "model/NaiveBayes"
    lr_path = "model/LogisticRegression"
    model_nb = PipelineModel.load(nb_path)
    model_lr = PipelineModel.load(lr_path)

    dataFrame = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("encoding", "utf-8")\
        .load("realtime/realtime_labelled.csv")
    
    predictData = dataFrame.select("tweetP", "Category")

    
    predictions_lr=model_lr.transform(predictData)
    predictions_lr_metrics=predictions_lr.select("label", "prediction").rdd
    lr_metrics = MulticlassMetrics(predictions_lr_metrics)

    print(lr_metrics.confusionMatrix().toArray())
    print(lr_metrics.falsePositiveRate(0.0))
    print(lr_metrics.falsePositiveRate(1.0))
    print(lr_metrics.falsePositiveRate(2.0))


    predictions_nb=model_nb.transform(predictData)
    predictions_nb_metrics=predictions_nb.select("label", "prediction").rdd
    nb_metrics = MulticlassMetrics(predictions_nb_metrics)

    print(nb_metrics.confusionMatrix().toArray())

    predictions_nb.toPandas().to_csv('realtime/realtime_nb_predictions.csv',mode='w')
    predictions_lr.toPandas().to_csv('realtime/realtime_lr_predictions.csv',mode='w')
