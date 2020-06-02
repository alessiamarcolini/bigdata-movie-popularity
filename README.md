# bigdata-movie-popularity
Big Data course Project

## Setup
Tested on Ubuntu 18.04 LTS.

### Install Java 8

```bash
$ sudo apt update
$ sudo apt install openjdk-8-jre-headless
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
$ service hadoop start
$ service hadoop stop
$ service hadoop start
```