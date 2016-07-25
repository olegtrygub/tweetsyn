#Tweetsyn - search better with synonyms

Search engines like Elasticsearch have problem with inability to find documents that don't contain exact search query term. 
Tweetsyn is a proof of concept project that shows how this problem can be tackled by getting synonyms from Word2Vec model and 
expanding search queries on the fly using Redis and SparkStreaming.


##Main files in the project are:

tweetsyn/src/main/scala/SparkStreamingSearch.scala  - provides percolating queries registration

tweetsyn/src/main/scala/SparkTweetsIndexer.scala - indexes documents into elasticsearch, percolates documents using registered queries

tweetsyn/src/main/scala/TwitterKafkaProducer.scala - provides live Twitter sample for the United States (english tweets)

##Test envinroment include:

tweetsyn/src/main/scala/TestRegisterQueries.scala - registeres queries for all terms in the the cached model

tweetsyn/src/main/scala/TestKafkaProducer.scala - provides "dummy" tweet producer with throttling

##Script for loading model 

tweetsyn/scripts/cacheModel.py - caches gensim model into Redis
