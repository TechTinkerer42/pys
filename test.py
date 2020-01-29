import logging
from logging.handlers import TimedRotatingFileHandler
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
import datetime
import re



class app:

    def __init__(self):
        print("Initializing app class")



def log(name, level=logging.INFO):
    form = logging.Formatter('%(asctime)s, %(levelname)s, %(message)s', '%Y-%m-%d, %H:%M:%S')
    logger = logging.getLogger(name)
    log_prefix=re.sub('[^0-9]', '_', str(datetime.datetime.now()).split('.')[0])
    if name.strip()=="ERROR":
        fhandler = TimedRotatingFileHandler(log_prefix+"_scecom_error.log", when="midnight", interval=1, backupCount=0,
                                            encoding=None, delay=False,utc=False, atTime=None)
    else:
        fhandler = TimedRotatingFileHandler(log_prefix+"_scecom.log", when="midnight", interval=1, backupCount=0,
                                            encoding=None,delay=False,utc=False, atTime=None)

    fhandler.setFormatter(form)
    chandler = logging.StreamHandler()
    chandler.setFormatter(form)
    logger.addHandler(fhandler)
    logger.addHandler(chandler)
    logger.setLevel(level)
    logadd=logging.getLogger(name.upper())
    return logadd



def logger(level='INFO',msg=None):
    try:
        assert isinstance(level,str)
        logger = logging.getLogger(str(msg))
        logger.setLevel(eval('logging.'+level))
        logger.addHandler(logging.StreamHandler)
        return logger.level('')
    except AssertionError as e:
        print("Please specify the level as string. The default will be INFO")



def get_random_words():
    import random
    words="this is just a test".split()
    while 'this' not in random.choice(words):
        continue
    return random.choice(words)



from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

hc = SparkSession.builder.enableHiveSupport().getOrCreate()
hc.sql('show tables from stg').show(1000,1000)
df1 = hc.table('stg.wd440_cpr_generation')

def profiler(table):
    print('PROFILING TABLE: '+str(table))
    table.unpersist()
    table.cache()
    for coll in table.columns:
        print('CHECKING MAX LENGTH')
        try:
            max_length=table.rdd.map(lambda x: len(str(x[coll]))).reduce(lambda x, y: x if x > y else y)
        except Exception as e:
            continue
        print('CHECKING MIN LENGTH')
        try:
            min_length=table.rdd.map(lambda x: len(str(x[coll]))).reduce(lambda x, y: x if x < y else y)
        except Exception as e:
            continue
        print('MAX LENGTH: '+str(max_length)+' MIN LENGTH: '+str(min_length))
        print('GROUP BY ON COLUMN: '+str(coll))
        groupBy=table.groupBy(coll).agg(count(coll).alias('c')).orderBy(col('c').desc())
        groupBy.show(10,1000)
        if groupBy.count() > 1:
            print('THE TABLE HAS NOT UNIQUE VALUES')
        else:
            print('THE TABLE HAS DISTINCT VALUES')
        print('ROW COUNT ON COLUMN: '+str(coll))
        table.withColumn('row_num', row_number().over(Window().partitionBy(coll).orderBy(coll))).filter(col('row_num')==1).show(10,100)
    table.unpersist()
    print('FINISHED PROCESSING TABLE: '+str(table))


profiler(df1)
















































def profiler(table):
    print('PROCESSING TABLE: '+str(table))
    table.unpersist()
    table.persist()
    for coll in table.columns:
            print('PROCESSING COLUMN: '+str(coll))
            table_fin=table.withColumn('rownum', row_number().over(Window().partitionBy(coll).orderBy(coll)))
            table_fin.groupBy(coll).agg(count(coll).alias('count')).orderBy(col('count').desc()).show(10,1000)
            tab=table_fin.select(coll).rdd
            tab.cache()
            tab.map(lambda x: len(str(x[column]))).reduce(lambda x,y: x if x>y else y).collect()
            tab.unpersist()
    table.unpersist()

w=Window().partitionBy(coll).orderBy(coll)
df1.withColumn('rownum', row_number().over(w))
from pyspark.sql.window import Window
from pyspark.sql.functions import *
df1.withColumn('rownum', row_number().over(Window().partitionBy(col('servicepointid')).orderBy(col('servicepointid')))).filter(col('rownum')==1)
casestat=[when(col('a')==col('b'), col('b')).otherwise('no match').alias('case')]

if __name__ == "__main__":
    conf = SparkConf().setAppName("Spark Session for profiling")
    sc = SparkContext(conf=conf)
    hc=SparkSession.builder.enableHiveSupport().getOrCreate()
    df1 = hc.sql("select * from table")
    df1.rdd.getNumPartitions()


# sorting FILE
#lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
#sortedCount = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (int(x), 1)).sortByKey()
# output = sortedCount.collect()
#for (num, unitcount) in sortedCount:
#    print(num)

