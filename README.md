# bigdata-movie-popularity
Big Data course Project

## Setup
Tested on Ubuntu 18.04 LTS.

### Install Java 8

```bash
$ sudo apt update
$ sudo apt install openjdk-8-jre-headless
$ echo 'export JAVA_HOME="/usr"' >> ~/.bashrc
```

### Install Scala
```bash
$ sudo apt install scala
```

### Install and setup docker
```bash
$ sudo apt install docker.io
$ sudo apt install docker-compose
$ sudo systemctl start docker
$ sudo systemctl enable docker
```

### Hadoop

#### Setup a local Hadoop cluster
```bash
$ docker pull segence/hadoop:latest
$ git clone https://github.com/alessiamarcolini/docker-hadoop
$ cd docker-hadoop/cluster-setup/local-cluster
$ docker-compose up -d
```

#### Starting the cluster
```bash
$ docker exec -it hadoop-namenode bash
hadoop-namenode$ su hadoop
hadoop-namenode$ bash ~/utils/format-namenode.sh
```

Execute the following commands as `root`:
```bash
hadoop-namenode$ service hadoop start
hadoop-namenode$ service hadoop stop
hadoop-namenode$ service hadoop start
```

### Download Spark
```bash
$ wget https://mirror.nohup.it/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
$ tar zxvf spark-2.4.5-bin-hadoop2.7.tgz -C ~/

$ echo 'export SPARK_HOME=$HOME/spark-2.4.5-bin-hadoop2.7' >> ~/.bashrc
```

### Install MongoDB
```bash
$ wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
$ sudo apt update
$ sudo apt install -y mongodb-org
$ sudo systemctl start mongod
$ sudo systemctl enable mongod
```


### Install Anaconda Python
```bash
$ wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh
$ bash Anaconda3-2020.02-Linux-x86_64.sh -b
$ conda update -y -n base conda
$ conda upgrade -y --all
```

#### Setup conda environment
```bash
$ git clone https://github.com/alessiamarcolini/bigdata-movie-popularity.git
$ cd bigdata-movie-popularity
$ conda env create -f env-yml
```