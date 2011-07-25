Word Count Test Adapter
=======================
This is a basic [Hadoop](http://hadoop.apache.org/) word count example refactored to make unit testing easy.
The mapper and reducer classes define `write()` methods which take key, value, and context arguments.
The unit test harness classes override these methods, passing in `null` for context and storing the key, value pairs.
This allows the algorithm to be tested outside of a running MapReduce program.