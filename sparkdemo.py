from pyspark import SparkConf, SparkContext

sc = SparkContext(master="local", appName="Spark Demo")

print(sc.textFile("C:\\hadoop\\deckofcards.txt").first())