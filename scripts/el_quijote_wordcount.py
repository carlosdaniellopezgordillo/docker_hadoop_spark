"""
    Ejecutar: 
    Opcion 1
    1.- docker exec -it spark-master bash /spark/bin/spark-submit --conf spark.pyspark.python=python3 /scripts/el_quijote_wordcount.py

    Opcion 2
    1.- docker exec -it spark-master bash
    2.- /spark/bin/spark-submit --conf spark.pyspark.python=python3 /scripts/el_quijote_wordcount.py
"""
from pyspark.sql import SparkSession

# Crear la sesion de Spark
spark = SparkSession.builder.appName("WordCount_El_Quijote").getOrCreate()

# Leer el archivo desde HDFS
text = spark.read.text("hdfs://namenode:9000/user/data/el_quijote.txt")

# Transformaciones estilo MapReduce
words = text.rdd.flatMap(lambda line: line.value.split())
word_pairs = words.map(lambda word: (word, 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Obtener y mostrar el top 10
top_10 = word_counts.takeOrdered(10, key=lambda x: -x[1])
for word, count in top_10:
    print(f"{word}: {count}")

# Guardar resultado completo en HDFS
word_counts.saveAsTextFile("hdfs://namenode:9000/user/data/output/el_quijote_wc_script.txt")

# Detener Spark
spark.stop()