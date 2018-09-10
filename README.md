# Dumbo
Dumbo is an international search engine :)

Our first phase is to crawl sites and build our database


Checkout our WIKI for more detail info

# Requirements:
Servers with hdfs, hbase, kafka, elastic search which their config should also put in our resources folder


# How to use:
Run search module, then "?l" command shows all possible commands.
0) ?l: Shows commands and their abbreviations.
1) search: Simple search, contains the whole phrase.
2) advanced-search: Has 3 options:

(a) must: Search result must contain the whole phrase 

(b) mustnot: Search result must not contain the whole phrase

(c) should: Search results containing the whole phrase will be ranked higher
 
 You may add more than one phrase to each of advanced-search's options.
 
 ## Rewrite later
 ### Elasticsearch index creation
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

analysis of performance:
hbase data bulk, hbase mark bulk, elastic bulk
HBase mark thread, HBase data thread, fetcher thread, parser thread, elastic thread : number of fetch and parse per second : testTime
5000, 5000, 500
16, 8, 200, 8, 4 : 150, 151 : 3065s

5000, 5000, 500
16, 8, 150, 8, 4 : 109, 109 : 710s

5000, 5000, 500
16, 8, 250, 8, 4 : 150, 148 : 2200s

5000, 5000, 1000
16, 8, 250, 8, 4 : 

    //https://www.csoonline.com/article/3238884/linux/linux-antivirus-and-anti-malware-8-top-tools.html





# Setting Kafka

```$xslt
./bin/kafka-server-start.sh -daemon config/server.properties
./bin/zkServer.sh start conf/zoo_hitler.cfg
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test


```