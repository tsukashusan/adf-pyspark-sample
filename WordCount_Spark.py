#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from operator import add

from pyspark.sql import SparkSession

def main():
    try:
        spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
        #sc = spark.sparkContext
        #sc._jsc.hadoopConfiguration().set('fs.azure.account.key.<StorageAccountName>.blob.core.windows.net', '<StorageAccountAccessKey>')
        lines = spark.read.text("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/inputfiles/minecraftstory.txt").rdd.map(lambda r: r[0])
        counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
        counts.saveAsTextFile("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/outputfiles/wordcount01")
        spark.stop()
    except OSError as err:
         print("OS error: {0}".format(err))
         sys.exit(5)
    except:
        errors = sys.exc_info()
        for err in errors:
            print("Unexpected error:", err)
        sys.exit(6)

if __name__ == "__main__":
    main()
    sys.exit(0)
