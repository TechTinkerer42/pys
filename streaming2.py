import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, DataFrame, SQLContext


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


# Convert RDDs of the words DStream to DataFrame and run SQL query
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        sqlContext = getSqlContextInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        #rowRdd = rdd.map(lambda w: Row(word=w))
        rowRdd = rdd.map(lambda w: Row(word=w[0], cnt=w[1]))
        #rowRdd.pprint()
        wordsDataFrame = sqlContext.createDataFrame(rowRdd)
        wordsDataFrame.show()

        # Creates a temporary view using the DataFrame.
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = \
             spark.sql("select SUM(cnt) as total from words")
        wordCountsDataFrame.show()
    except:
       pass

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_dataframes.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 1)


    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    words = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    words.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()