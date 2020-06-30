# Ko-partitioning
###### Co-partitioning requirements illustrated 

This module is the code example to the article [Co-partitioning requirements illustrated](
https://medium.com/xebia-france/kafka-streams-co-partitioning-requirements-illustrated-2033f686b19c)

Motivation: Kafka Streams let's you perform joins of multiple streams in real time, but how does this works?
In this context of distributed data, how Kafka Streams instances bring matching record together 
without any direct shuffle?  

This module illustrates one of the properties that makes this possible.

## Requirements:
- Jdk1.8+
- Docker
- watch
