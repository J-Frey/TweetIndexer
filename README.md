# TweetIndexer
The Tweet Indexer is a Hadoop based project that processes word occurance, frequency, and positions in a set of Tweets. The resulting JSON Objects can be loaded into a web service (sold separately!) as an index for searching. The frequency and word positions can be used in various ranking and search algorithms within the web service.

This project was originally used in a university class on Information Retrieval.
The [TweetCollector](https://github.com/J-Frey/TweetCollector) project can be used alongside this project as a toolchain.

**NOTE:** This project is "AS IS" and may not work properly for all Hadoop configurations. I have no interest in improving or supporting this code.

## Getting Started
::TODO:: 

### Prerequitisites
* Hadoop 2 and it's requirements
* twitter4j-core-4.0.3 (installed on the HFS)
* A bunch of Twitter Data [(click here to collect some!)](https://github.com/J-Frey/TweetCollector)

### Installation
::TODO::

### Input Data
::TODO::

### Execution
The scripts included simplify the execution process:
```
$ ./make.sh
$ ./run.sh
```
## Output Data
::TODO::

## Authors
* **JFrey** - *Initial work* - [GitHub](https://github.com/J-Frey)