from pyspark import SparkConf,SparkContext
def p(x):

    return  (x[1],x[0])
if __name__ == '__main__':

    conf = SparkConf().setAppName("zsx_wordCount").setMaster("local")
    sc = SparkContext(conf=conf)
    path ="./words.txt"
    line = sc.textFile(path)
    #line.map(lambda x: p(x)).foreach(print)
    line.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).map(lambda x:p(x)).sortByKey(ascending=False).map(lambda x:p(x)).foreach(print)