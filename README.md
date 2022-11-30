### An example of using Jedis to interact with Redis Streams
#### In this example, 
1) food_order entries are written to a redis stream
2) a worker in a workerGroup processes those entries by converting them into Redis hashes

The default command uses localhost:6379 and writes 10 entries to the stream - which then get transformed by the worker into Hash objects

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false
```

You can specify how many entries to create and write into the stream using --howmany

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100"
```

Here is an example of passing in user and password and alternate host and port:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100 --host 192.168.1.1 -port 12000 --username gerard --password &pTRsFE$E"
```
