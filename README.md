# Ko-partitioning
###### Co-partitioning requirements illustrated 

![Master Workflow](https://github.com/DivLoic/ko-partitioning/workflows/Master%20Workflow/badge.svg)

This module is the code example to the article [Co-partitioning requirements illustrated](
https://medium.com/xebia-france/kafka-streams-co-partitioning-requirements-illustrated-2033f686b19c)

Motivation: Kafka Streams lets you perform joins of multiple streams in real time, but how does this works?
In this context of distributed data, how Kafka Streams instances bring matching record together 
without any direct shuffle?  

This module illustrates one of the properties that makes this possible.

## Requirements:
- Jdk1.8+
- Docker
- watch

## Demo steps
Each steps of the demo has its own branch and goes through how partitions affect a Kafka Streams application.

### Step 0
Let's say for example you try to join to streams (KStreams-KTable) without any processing. 
In this case Kafka Streams has your back!

[![asciicast](https://asciinema.org/a/331595.svg)](https://asciinema.org/a/331595)
  
### Step 1
### Step 2
### Step 3
### Step 4

