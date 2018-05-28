#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from operator import add
from datetime import datetime as dt
import pytz

from pyspark.sql import SparkSession

BLOB_PATH = "wasbs://<storageContainer>@<storageAccount>.blob.core.windows.net"
APP_NAME = "PythonWordCount"
INPUT_PATH = "%s/spark/inputfiles/minecraftstory.txt" % BLOB_PATH
OUTPUT_PATH = "%s/spark/outputfiles/wordcount%s" % (BLOB_PATH, "%s")


def main():
    spark = None
    try:
        strdate = dt.now(pytz.timezone('Asia/Tokyo')).strftime('%Y%m%d%H%M%s')
        spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
        lines = spark.read.text(INPUT_PATH).rdd.map(lambda r: r[0])
        counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
        filePath = OUTPUT_PATH % strdate
        counts.saveAsTextFile(filePath)
        spark.stop()
    except OSError as err:
         print("OS error: {0}".format(err))
         spark.stop()
         sys.exit(5)
    except:
        errors = sys.exc_info()
        for err in errors:
            print("Unexpected error:", err)
        sys.exit(6)

if __name__ == "__main__":
    main()
    sys.exit(0)
