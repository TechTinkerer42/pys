import sys
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import column
from pyspark.sql.types import *
from pyspark.sql.function import *
import logging

db=sys.argv[1]
table=sys.argv[2]
file_path=sys.argv[3]

job_name="ProfilingPySpark"

class Profiler:
        def __init__(self):
                logging.log(1,'Class initialized')

        def logger(self, level, msg):
                if str(level).strip() == 'INFO':
                        return logging.log()
                elif str(level).strip() == 'DEBUG':

                elif str(level).strip() == 'WARN':

                elif str(level).strip() == 'ERROR':

                else:
                        logging.log(1,'ERROR IN THE LOGGER DEFINITION')
                        exit(1)

        def run(self,df,column,f):
                try:
                        sep="|"
                        Profiler.logger('INFO','')
                        rdd=df.select(str(column)).rdd.persist()
                        df
                        rddCount=rdd.count()
                        rddDistinctCount=rdd.distinct().count()
                        isDistinct="TRUE" if rddCount==rddDistinctCount else "FALSE"
                        s=rdd.filter(lambda x:len((str(x[column]).strip()))==0).count()
                        hasBlanks="FALSE" if str(blanks)=="0" else "TRUE"
                        nulls=rdd.filter(lambda x: x[column]==None).count()
                        hasNulls="FALSE" if(str(nulls)=="0") else "TRUE"
                        maxLength=rdd.map(lambda x: len(str(x[column]))).reduce(lambda x,y: x if x > y else y)
                        minLength=rdd.map(lambda x: len(str(x[column]))).reduce(lambda x,y: x if x < y else y)
                        rdd.unpersist()
                        f.write(db+sep+table+sep+column+sep+isDistinct+sep+hasBlanks+sep+hasNulls+sep+str(maxLength)+sep+str(minLength)+"\n")
                except Exception as e:
                        logging.log(1,'PROCESSING FAILED')
                        print(repr(e))

        def tableColumns(db,table):
                try:
                        with open(file_path,"w") as f:
                                tablename=db + '.' + table
                                df=hc.table(str(tablename))
                                columns_list=df.columns
                                for col in columns_list:
                                        Profiler.run(df,col,f)
                except Exception as e:
                        logging.log(1,'ERROR found while processing the dataset')


        def profiler2(self, table):
                from pyspark.sql.function import *
                print("PROCESSING TABLE: "+ str(table))
                table.persist()
                for coll in table.columns:
                        print("Processing column "+ coll + " in table " + str(table))
                        table.groupBy(coll).agg(count(coll).alias('c')).orderBy(col('c').desc()).show(10,1000)
                        print("Finding max length fro column: "+ coll)
                        table.rdd.map(lambda x: len(str(x[column]))).reduce(lambda x,y: x if x>y else y).take(10)
                        print("Finding min length fro column: "+ coll)
                        table.rdd.map(lambda x: len(str(x[column]))).reduce(lambda x,y: x if x<y else y).take(10)
                table.unpersist()

if __name__ == '__main__':
        conf = SparkConf().setAppName(job_name)
        sc = SparkContext(conf=conf)
        sc.setJobGroup(job_name, "PYTHON PROFILER")
        hc=SparkSession.builder.enableHiveSupport().getOrCreate()
        #hc=HiveContext(sc)
        prof=Profiler
        prof.tableColumns(str(db),str(table))

