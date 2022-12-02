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
mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100 --host 192.168.1.1 -port 12000 --user gerard --password &pTRsFE$E"
```

You can also change 
1) the block time for the workers as they wait for new entries to appear in the stream
2) the number of entries for each worker to accept from the stream on each read

``` 
mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 50 --processeachread 10 --blocktimeeachread 10000"
```

#### A natural next step would be to create a search index on top of the hashes and then use RediSearch to get a better understanding of what is being ordered

#### If you feel so inclined - and have a Redis Enterprise instance running with the Search module 

#### You can create a search schema like the following:

```
FT.CREATE idx_foodOrders PREFIX 1 foodOrder: SCHEMA entree TEXT PHONETIC dm:en personID TAG delivery_time TAG delivery_day TAG dessert TEXT PHONETIC dm:en appetizer TEXT PHONETIC dm:en beverage TEXT PHONETIC dm:en 
```

#### This will allow you to search like this:

```
FT.SEARCH idx_foodOrders "forest Aiel" return 3 dessert beverage personID LIMIT 0 2
1) (integer) 1
2) "foodOrder:1669784944842-3"
3) 1) "dessert"
   2) "Black Forest Cake"
   3) "beverage"
   4) "Ginger Ale"
   5) "personID"
   6) "34:27029"
```

And this:

``` 
FT.AGGREGATE idx_foodOrders * GROUPBY 1 @dessert reduce count 0 as dessert_popularity  
1) (integer) 7
2) 1) "dessert"
   2) "French Toast Sundae"
   3) "dessert_popularity"
   4) "15"
3) 1) "dessert"
   2) "Mochi"
   3) "dessert_popularity"
   4) "18"
4) 1) "dessert"
   2) "Red Jello and Cookies"
   3) "dessert_popularity"
   4) "16"
5) 1) "dessert"
   2) "Fruit Crepe"
   3) "dessert_popularity"
   4) "18"
6) 1) "dessert"
   2) "Black Forest Cake"
   3) "dessert_popularity"
   4) "27"
7) 1) "dessert"
   2) "Ginger Rice Pudding"
   3) "dessert_popularity"
   4) "19"
8) 1) "dessert"
   2) "Banana Split"
   3) "dessert_popularity"
   4) "10"
```


Streams cheat sheet: https://lp.redislabs.com/rs/915-NFD-128/images/DS-Redis-Streams.pdf 

Search Info: https://redis.io/commands/ft.search/ 

https://redis.io/commands/ft.aggregate/ 
