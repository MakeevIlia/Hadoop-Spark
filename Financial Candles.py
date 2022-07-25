import pyspark
import sys
import argparse
import datetime as d
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from math import sqrt


def arguments():
    arg = argparse.ArgumentParser()
    arg.add_argument('--securities', default='SRZ1|SPZ1', type=str)
    arg.add_argument('--date_from', default='20110111', type=str)
    arg.add_argument('--date_to', default='20110112', type=str)
    arg.add_argument('--time_from', default=1000, type=int)
    arg.add_argument('--time_to', default=1015, type=int)
    arg.add_argument('--widths', default='5,10', type=str)
    arg.add_argument('--shifts', default='0,1,2', type=str)
    arg.add_argument('--num_reducers', default=1, type=int)
    return arg


def format(data):
    return ','.join(str(d) for d in data)


def Mapper(line):
    res = []
    parts = line.split(",")
    ca = d.datetime(int(parts[2][:4]), int(parts[2][4:6]), int(parts[2][6:8]), int(parts[2][8:10]), int(parts[2][10:12])
                    , int(parts[2][12:14]), int(parts[2][14:]) * 1000)
    cb = d.datetime(int(pars.date_from[:4]), int(pars.date_from[4:6]), int(pars.date_from[6:8]), pars.time_from // 100,
                    pars.time_from % 100)
    lwidths = pars.widths.split(",")
    for widths in lwidths:
        num = int((ca - cb).total_seconds() * 1000 // int(widths))
        key = parts[0] + "," + str(num) + "," + widths
        val = parts[2] + "," + parts[3] + "," + parts[4]
        res.append((key, val))
    return res


def Reducers(value1, value2):
    a = value1.split(",")
    b = value2.split(",")
    time = int(b[0][8:17])
    id = int(b[1])
    price = float(b[2])
    price_open = float(a[0])
    price_close = float(a[3])
    time_open = int(a[4])
    id_close = int(a[5])
    id_open = int(a[6])
    time_close = int(a[7])
    if time_open > time:
        time_open = time
        id_open = id
        price_open = price
    elif time_open == time:
        if id_open > id:
            id_open = id
            price_open = price
    if time_close < time:
        time_close = time
        id_close = id
        price_close = price
    elif time_close == time:
        if id_close < id:
            id_close = id
            price_close = price
    val = str(price_open) + "," + max(a[1], b[2]) + "," + min(a[2], b[2]) + "," + str(price_close) + "," + \
          str(time_open) + "," + str(id_close) + "," +str(id_open) + "," +str(time_close)
    return val


def corr(a, b):
    x = []
    y = []
    for i in range(len(a) - 1):
        x.append((a[i + 1] - a[i]) / a[i])
        y.append((b[i + 1] - b[i]) / b[i])
    sx = 0
    sy = 0
    for i in range(len(x)):
        sx += x[i] / len(x)
        sy += y[i] / len(y)
    s2 = 0
    s3 = 0
    s4 = 0
    for i in range(len(x)):
        s2 += (x[i] - sx) * (y[i] - sy)
        s3 += (x[i] - sx) ** 2
        s4 += (y[i] - sy) ** 2
    return s2 / (sqrt(s3 * s4))


conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf).getOrCreate()
sc.setLogLevel("WARN")
conf.set("spark.hadoop.validateOutputSpecs", "false")
inputfile = sys.argv[1]
outputdir = sys.argv[2]
parser = arguments()
pars = parser.parse_args(sys.argv[3:])
text = sc.textFile(inputfile)
widths = pars.widths.split(",")
KeyValue = text.filter(lambda line: line.split(",")[0] in pars.securities.split("|")
                and (int((line.split(",")[2])[:8]) >= int(pars.date_from)) and
                (int((line.split(",")[2])[:8]) <= int(pars.date_to))
                and (int((line.split(",")[2])[8:]) >= pars.time_from * 100000) and
                (int((line.split(",")[2])[8:]) <= pars.time_to * 100000)).flatMap(Mapper)
init = "0,0," + str(sys.maxsize) + ",0," + str(pars.time_to * 100000) + ",0," \
       + str(sys.maxsize) + "," + str(pars.time_from * 100000)
candles = KeyValue.foldByKey(init, Reducers, numPartitions=pars.num_reducers)
spark = SparkSession.builder.appName("Name").getOrCreate()
DataFrame = spark.createDataFrame(candles)
DataFrame.show()
DataFrame1 = DataFrame.select('_2', func.split('_1', ",").alias('_1'))
DataFrame2 = DataFrame1.select('_2', *[DataFrame1['_1'][i] for i in range(3)])
DataFrame3 = DataFrame2.select('_1[0]', '_1[1]', '_1[2]', func.split('_2', ",").alias('_2'))
FinalDF = DataFrame3.select('_1[0]', '_1[1]', '_1[2]', *[DataFrame3['_2'][3]])
FinalDF.show()
Old = FinalDF.schema.names
New = ["symbol", "moment", "width", "price_close"]
for i in range(len(New)):
    FinalDF = FinalDF.withColumnRenamed(Old[i], New[i])
DataArray = []
for widths in pars.widths.split(","):
    DataArray.append([])
    for Instr in sorted(pars.securities.split("|")):
        Cache = FinalDF.filter((FinalDF.width == widths) and
                              (FinalDF.symbol == Instr)).select("moment", "price_close")
        DataArray[len(DataArray) - 1].append(Cache)
JoinMatrix = []
for q in DataArray:
    JoinMatrix.append([])
    for i in range(len(q)):
        for j in range(i+1, len(q)):
            temp1 = q[i].withColumnRenamed("price_close", "Instr1")
            temp2 = q[j].withColumnRenamed("price_close", "Instr2")
            full_outer_join = temp1.join(temp2, on=["moment"], how='full').sort("moment")
            JoinMatrix[len(JoinMatrix) - 1].append(full_outer_join)
Lwidths = pars.widths.split(",")
Linstr = sorted(pars.securities.split("|"))
Data = []
Col = 0
Row = 0
for q in JoinMatrix:
    for t in q:
        tm = t.filter((t.Instr1 != 'null') & (t.Instr2 != 'null'))
        x = [float(id) for id in [str(row.Instr1) for row in tm.collect()]]
        y = [float(id) for id in [str(row.Instr2) for row in tm.collect()]]
        corr = corr(x, y)
        Data.append(Linstr[Col] + " " + Linstr[Row] + " " + Lwidths[Col] + str(corr))
        Row += 1
    Col += 1
Txt = spark.sparkContext.parallelize(Data)
Line = Txt.map(format)
Line.saveAsTextFile(outputdir)
St = StructType([
    StructField('sec1', StringType(), True),
    StructField('sec2', StringType(), True),
    StructField('Width', StringType(), True),
    StructField('Correlation', StringType(), True)
])
SaveDF = spark.createDataFrame(Txt, St)
Txt.write.parquet("parquet_result/corr.parquet")
