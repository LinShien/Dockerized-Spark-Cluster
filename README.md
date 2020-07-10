# Dockerized Spark Cluster

> 利用 Docker 建立一個本地端的 Spark Cluster 
> Dockerfile 來自 [mvillarrealb](https://github.com/mvillarrealb/docker-spark-cluster "link") 的 github 且改成 Spark 3.0.0 並加裝 Pyspark 

<img src="https://spark.apache.org/images/spark-logo-trademark.png">

## Components
  - Spark Master Image (Spark master and cluster manager)
  - Spark Worker Image (Spark worker)
  - Spark submit Image (container that submit your application to cluster)

## Prerequisite
  - Docker 19.03.6
  - Docker-compose 1.23.2
  - Python > 3.6
## Set up
### 1.Build images   
```sh
$ sh build-images.sh
$ docker images   # check the images you just build
```

<img src="https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/images/docker_images.png">

### 2.Run all containers
```sh
$ docker-compose up --scale spark-worker=3
```
<img src="https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/images/docker_ps.png">

##### To check your cluster is alive or not, to open the spark cluster web UI, type the following in your browser
[localhost:9090](https:localhost:9090)

<img src="https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/images/webUI.png">

<img src="https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/images/spark_master.png">

### To Run Your Spark Application
##### Your have to run a submit container to submit your app to spark cluster 
> 這裡我用修改後的 [sparkLoader.py](https://github.com/LinShien/Covid19-Analytics-With-Spark/blob/master/sparkLoader.py) 做測試
```sh
$ SPARK_PY_APPLICATION=Path-Of-Your-App-In-Locally

$ docker run --network docker-spark-cluster_default 
-v /home/size7311/docker-spark-cluster/apps:/opt/spark-apps \
-v /home/size7311/docker-spark-cluster/data:/opt/spark-data \
--env SPARK_PY_APPLICATION=$SPARK_PY_APPLICATION \ 
spark-submit:latest
```

<img src="https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/images/result.png">

### To tune the performance
  - Deploy the worker node on different 'machines' instead of a local machine
  - Modify the [docker/spark-submit/spark-submit.sh](https://github.com/LinShien/Dockerized-Spark-Cluster/blob/master/docker/spark-submit/spark-submit.sh)
  
