# Dumbo

## What is it?
Dumbo is the first search engine for the whole World Wide Web made by Jimbo group. It consists of several modules:

* crawler - crawls the web pages that are in English
* search - the search API and command line interface for the project
* webui - the GUI of the search engine
* ranker - extracts number of references for each page and also compute their Page Rank
* newsfetcher - the RSS feed reader which stores the data for later use
* keywords - computes and stores the keywords for every page
* anchor - extracts top anchors for every link and stores them to influence searching
* domaingraph - computes the necessary data to show the graph
* commons - the common classes which are shared between modules
* twitter-spark - handles the twitter data stream
* scripts - other scripts that are not in Java

This project uses big data technologies like HDFS, HBase, Elasticsearch, Kafka, Spark, Zookeeper, etc.

## Prerequisites
Zookeeper, Hadoop, HBase, Elasticsearch, Spark and Kafka should be up and running before running the modules. There
are instructions in wiki section to help you bring them up.
### Kafka
```$xslt
$KAFKA_HOME/bin/kafka-server-start.sh -daemon config/server.properties
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic UrlFrontier
```
The name of the topic should be set later in config files.
### HBase
There is no need to create the table beforehand; the code will take of auto-creation of main tables itself if you run
the crawler for the first time.

If you wish to create the tables yourself, or if crawler is not the first module you wish to run (in this case some
tables may not be created), run the following command in HBase shell to create the main table.
```$xslt
create '<table-name>', 'Data', 'Meta'
```
The table name should be configured in config files as well.
### Elasticsearch
Before running, an index with the following description must be created: 
```json
 {
   "settings" : {
     "number_of_shards" : 6,
     "number_of_replicas" : 1,
     "analysis": {
        "filter": {
          "english_stop": {
            "type":       "stop",
            "stopwords":  "_english_" 
          },
          "english_keywords": {
            "type":       "keyword_marker",
            "keywords":   ["the who"] 
          },
          "english_stemmer": {
            "type":       "stemmer",
            "language":   "english"
          },
          "english_possessive_stemmer": {
            "type":       "stemmer",
            "language":   "possessive_english"
          }
        },
        "analyzer": {
          "rebuilt_english": {
            "tokenizer":  "standard",
            "filter": [
              "english_possessive_stemmer",
              "lowercase",
              "english_stop",
              "english_keywords",
              "english_stemmer"
            ]
          }
        }
      }  
   },
   "mappings": {
     "_doc": {
       "properties": {
         "content": {
           "type": "text",
           "term_vector": "yes",
           "analyzer" : "rebuilt_english"
         },
         "description": {
           "type": "text"
         },
         "title": {
           "type": "text",
           "fields": {
             "keyword": {
               "type": "keyword",
               "ignore_above": 2048
             }
           }
         },
         "url": {
           "type": "keyword"
         },
         "anchor": {
           "type": "text",
           "fields": {
             "keyword": {
               "type": "keyword",
               "ignore_above": 2048
             }
           }
         }
       }
     }
   }
 }
```

Also every module has its own configuration files located at its resources directory. Set each field according to
your cluster configuration. 

## How To Use
### Packaging the project
Run this command at dumbo root to package the whole project:
```$xslt
mvn package
```
If you wish to package just one (or several) modules, run this command at dumbo root:
```$xslt
mvn package -pl <module> -am
```
Note that some tests may take a long time to pass; so in order to skip the test, you can use `-DskipTests` option
at the end of maven commands.

### Other tests
There are some test bash scripts available at `scripts` directory.

### Running the crawler
To put the seed links into the URL frontier, run the following command.
```$xslt
java -jar crawler/target/crawler-1.0-SNAPSHOT-jar-with-dependencies.jar seeder
```
Next, you need to run the crawler on each server. The management options are there to allow JMX metrics to expose 
themselves.
```$xslt
java -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -Djava.rmi.server.hostname=<hostname-or-ip> \
     -Dcom.sun.management.jmxremote.port=9120 \
     -jar crawler/target/crawler-1.0-SNAPSHOT-jar-with-dependencies.jar
``` 
TODO: managing the workers

### Monitoring
There are plenty of metrics that have been available via JMX. You can use JConsole, Zabbix, or other monitoring 
programs to watch how the crawler behaves.

### Searching
TODO

### Improving search results
**Use of anchors**: 
The `anchor` module collects the anchors for each page, and stores them in Elasticsearch to improve search results. You
just need to run the program. There is no need to set any environment variables, just the config files.

**Use of keywords**:
The `keywords` module computes the important keywords for each document and stores them in HBase to be presented in the
search results later. This program should be run normally.

**Rankings**:
The `ranker` module is to set a score for each page we have crawled. Right now, it supports computing the number of 
references for each link. This number is later used to sort the search results by their importance. Note that Page
Rank is not currently implemented. To run the program, you should submit its JAR file to Spark master.
```
$SPARK_HOME/bin/spark-submit --master spark://<spark-master> ranker/target/ranker-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Running and searching the news fetcher
TODO