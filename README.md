![Logo_compuesto_vof_2025](https://github.com/user-attachments/assets/df7db686-cfb0-4b47-a125-8cc28e402332)

Este repositorio fue utilizado para mostrar el uso de **Hadoop y Spark** (con Docker) para la materia *Herramientas Avanzadas para el Manejo de Grandes Volúmenes de Datos* de la Licenciatura en Ciencia de Datos para Negocios (LCDN) de la Universidad Nacional Rosario Castellanos (UNRC).

# Docker + Hadoop + PySpark

Contenedor de Docker con Hadoop (HDFS) y Spark. Pero sin los grandes requisitos de memoria de un sandbox de Cloudera.  

## Pre-requisitos
Tener Docker Desktop instalado para Windows y MacOs. Para el caso de Google Chromebook debe tener instalado Docker (vía terminal) 

**Observación:**  
Los comandos utilizados en este repositorio se ejecutaron en los siguientes sistemas operativos (+ Docker):  
* _Windows 11_,  
* _macOS_ (con chip Intel y M2), y  
* Google Chromebook.

## Clonar repositorio
#### Opción 1 (Manual)
 1.- Hacer click en el botón __<> CODE__  
 2.- Hacer click en el botón __Download ZIP__   
<img width="407" alt="image" src="https://github.com/user-attachments/assets/ec0e5f1e-3254-4d2b-9b69-f19c88e9c205" />

#### Opción 2 (Terminal) 
Ejecutar  
```
  git clone https://github.com/mevangelista-alvarado/docker_hadoop_spark.git 
```

## Hadoop Docker
Para ejecutar los siguientes comandos, debe ubicarse en la carpeta que contenga el archivo `docker-compose.yml`.

### Iniciar el contenedor
Para desplegar el ejemplo de un clúster HDFS, ejecute en la consola de su computadora (según el caso, si es la primera vez o no) lo siguiente:

#### Instalar el contenedor por primera vez
```
docker-compose up
```

#### Levantar el contenedor si ya está instalado
```
docker-compose start
```

#### Verificar que el contenedor está trabajando
Para comprobar que el contenedor está en funcionamiento:
```
docker-compose ps
```
Si el estado es `UP`, entonces el contenedor está trabajando correctamente. De lo contrario, ha ocurrido un error al inicializarlo.

#### Apagar el contenedor
```
docker-compose stop
```
**Observación:**  
Ejecute 
```
  docker network inspect
```
para encontrar la IP donde se publican las interfaces de Hadoop. Acceda a estas interfaces con las siguientes URL:

* Namenode: http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview
* History server: http://<dockerhadoop_IP_address>:8188/applicationhistory
* Datanode: http://<dockerhadoop_IP_address>:9864/
* Nodemanager: http://<dockerhadoop_IP_address>:8042/node
* Resource manager: http://<dockerhadoop_IP_address>:8088/
* Spark master: http://<dockerhadoop_IP_address>:8080/
* Spark worker: http://<dockerhadoop_IP_address>:8081/

## Quick Start

To deploy an the HDFS-Spark-Hive cluster, run:
```
  docker-compose up
```

`docker-compose` creates a docker network that can be found by running `docker network list`, e.g. `docker-hadoop-spark-hive_default`.

Run `docker network inspect` on the network (e.g. `docker-hadoop-spark-hive_default`) to find the IP the hadoop interfaces are published on. Access these interfaces with the following URLs:



## Important note regarding Docker Desktop
Since Docker Desktop turned “Expose daemon on tcp://localhost:2375 without TLS” off by default there have been all kinds of connection problems running the complete docker-compose. Turning this option on again (Settings > General > Expose daemon on tcp://localhost:2375 without TLS) makes it all work. I’m still looking for a more secure solution to this.


## Quick Start HDFS

Copy breweries.csv to the namenode.
```
  docker cp breweries.csv namenode:breweries.csv
```

Go to the bash shell on the namenode with that same Container ID of the namenode.
```
  docker exec -it namenode bash
```


Create a HDFS directory /data//openbeer/breweries.

```
  hdfs dfs -mkdir -p /data/openbeer/breweries
```

Copy breweries.csv to HDFS:
```
  hdfs dfs -put breweries.csv /data/openbeer/breweries/breweries.csv
```


## Quick Start Spark (PySpark)

Go to http://<dockerhadoop_IP_address>:8080 or http://localhost:8080/ on your Docker host (laptop) to see the status of the Spark master.

Go to the command line of the Spark master and start PySpark.
```
  docker exec -it spark-master bash

  /spark/bin/pyspark --master spark://spark-master:7077
```

Load breweries.csv from HDFS.
```
  brewfile = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")
  
  brewfile.show()
+----+--------------------+-------------+-----+---+
| _c0|                 _c1|          _c2|  _c3|_c4|
+----+--------------------+-------------+-----+---+
|null|                name|         city|state| id|
|   0|  NorthGate Brewing |  Minneapolis|   MN|  0|
|   1|Against the Grain...|   Louisville|   KY|  1|
|   2|Jack's Abby Craft...|   Framingham|   MA|  2|
|   3|Mike Hess Brewing...|    San Diego|   CA|  3|
|   4|Fort Point Beer C...|San Francisco|   CA|  4|
|   5|COAST Brewing Com...|   Charleston|   SC|  5|
|   6|Great Divide Brew...|       Denver|   CO|  6|
|   7|    Tapistry Brewing|     Bridgman|   MI|  7|
|   8|    Big Lake Brewing|      Holland|   MI|  8|
|   9|The Mitten Brewin...| Grand Rapids|   MI|  9|
|  10|      Brewery Vivant| Grand Rapids|   MI| 10|
|  11|    Petoskey Brewing|     Petoskey|   MI| 11|
|  12|  Blackrocks Brewery|    Marquette|   MI| 12|
|  13|Perrin Brewing Co...|Comstock Park|   MI| 13|
|  14|Witch's Hat Brewi...|   South Lyon|   MI| 14|
|  15|Founders Brewing ...| Grand Rapids|   MI| 15|
|  16|   Flat 12 Bierwerks| Indianapolis|   IN| 16|
|  17|Tin Man Brewing C...|   Evansville|   IN| 17|
|  18|Black Acre Brewin...| Indianapolis|   IN| 18|
+----+--------------------+-------------+-----+---+
only showing top 20 rows

```



## Quick Start Spark (Scala)

Go to http://<dockerhadoop_IP_address>:8080 or http://localhost:8080/ on your Docker host (laptop) to see the status of the Spark master.

Go to the command line of the Spark master and start spark-shell.
```
  docker exec -it spark-master bash
  
  spark/bin/spark-shell --master spark://spark-master:7077
```

Load breweries.csv from HDFS.
```
  val df = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")
  
  df.show()
+----+--------------------+-------------+-----+---+
| _c0|                 _c1|          _c2|  _c3|_c4|
+----+--------------------+-------------+-----+---+
|null|                name|         city|state| id|
|   0|  NorthGate Brewing |  Minneapolis|   MN|  0|
|   1|Against the Grain...|   Louisville|   KY|  1|
|   2|Jack's Abby Craft...|   Framingham|   MA|  2|
|   3|Mike Hess Brewing...|    San Diego|   CA|  3|
|   4|Fort Point Beer C...|San Francisco|   CA|  4|
|   5|COAST Brewing Com...|   Charleston|   SC|  5|
|   6|Great Divide Brew...|       Denver|   CO|  6|
|   7|    Tapistry Brewing|     Bridgman|   MI|  7|
|   8|    Big Lake Brewing|      Holland|   MI|  8|
|   9|The Mitten Brewin...| Grand Rapids|   MI|  9|
|  10|      Brewery Vivant| Grand Rapids|   MI| 10|
|  11|    Petoskey Brewing|     Petoskey|   MI| 11|
|  12|  Blackrocks Brewery|    Marquette|   MI| 12|
|  13|Perrin Brewing Co...|Comstock Park|   MI| 13|
|  14|Witch's Hat Brewi...|   South Lyon|   MI| 14|
|  15|Founders Brewing ...| Grand Rapids|   MI| 15|
|  16|   Flat 12 Bierwerks| Indianapolis|   IN| 16|
|  17|Tin Man Brewing C...|   Evansville|   IN| 17|
|  18|Black Acre Brewin...| Indianapolis|   IN| 18|
+----+--------------------+-------------+-----+---+
only showing top 20 rows

```

How cool is that? Your own Spark cluster to play with.


## Quick Start Hive

Go to the command line of the Hive server and start hiveserver2

```
  docker exec -it hive-server bash

  hiveserver2
```

Maybe a little check that something is listening on port 10000 now
```
  netstat -anp | grep 10000
tcp        0      0 0.0.0.0:10000           0.0.0.0:*               LISTEN      446/java

```

Okay. Beeline is the command line interface with Hive. Let's connect to hiveserver2 now.

```
  beeline -u jdbc:hive2://localhost:10000 -n root
  
  !connect jdbc:hive2://127.0.0.1:10000 scott tiger
```

Didn't expect to encounter scott/tiger again after my Oracle days. But there you have it. Definitely not a good idea to keep that user on production.

Not a lot of databases here yet.
```
  show databases;
  
+----------------+
| database_name  |
+----------------+
| default        |
+----------------+
1 row selected (0.335 seconds)
```

Let's change that.

```
  create database openbeer;
  use openbeer;
```

And let's create a table.

```
CREATE EXTERNAL TABLE IF NOT EXISTS breweries(
    NUM INT,
    NAME CHAR(100),
    CITY CHAR(100),
    STATE CHAR(100),
    ID INT )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/data/openbeer/breweries';
```

And have a little select statement going.

```
  select name from breweries limit 10;
+----------------------------------------------------+
|                        name                        |
+----------------------------------------------------+
| name                                                                                                 |
| NorthGate Brewing                                                                                    |
| Against the Grain Brewery                                                                            |
| Jack's Abby Craft Lagers                                                                             |
| Mike Hess Brewing Company                                                                            |
| Fort Point Beer Company                                                                              |
| COAST Brewing Company                                                                                |
| Great Divide Brewing Company                                                                         |
| Tapistry Brewing                                                                                     |
| Big Lake Brewing                                                                                     |
+----------------------------------------------------+
10 rows selected (0.113 seconds)
```

There you go: your private Hive server to play with.


## Configure Environment Variables

The configuration parameters can be specified in the hadoop.env file or as environmental variables for specific services (e.g. namenode, datanode etc.):
```
  CORE_CONF_fs_defaultFS=hdfs://namenode:8020
```

CORE_CONF corresponds to core-site.xml. fs_defaultFS=hdfs://namenode:8020 will be transformed into:
```
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
```
To define dash inside a configuration parameter, use triple underscore, such as YARN_CONF_yarn_log___aggregation___enable=true (yarn-site.xml):
```
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
```

The available configurations are:
* /etc/hadoop/core-site.xml CORE_CONF
* /etc/hadoop/hdfs-site.xml HDFS_CONF
* /etc/hadoop/yarn-site.xml YARN_CONF
* /etc/hadoop/httpfs-site.xml HTTPFS_CONF
* /etc/hadoop/kms-site.xml KMS_CONF
* /etc/hadoop/mapred-site.xml  MAPRED_CONF

If you need to extend some other configuration file, refer to base/entrypoint.sh bash script.
