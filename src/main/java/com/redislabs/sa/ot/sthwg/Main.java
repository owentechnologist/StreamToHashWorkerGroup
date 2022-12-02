package com.redislabs.sa.ot.sthwg;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.providers.PooledConnectionProvider;
import redis.clients.jedis.resps.StreamEntry;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;

/**
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100"
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100 --host 192.168.1.1 -port 12000 --user gerard --password &pTRsFE$E"
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 50 --processeachread 10 --blocktimeeachread 10000"
 */
public class Main {
    static String host = "localhost";
    static int port = 6379;
    static String username = "default";
    static String password = "";
    static URI jedisURI = null;
    static ConnectionHelper connectionHelper = null;
    static String streamKeyName = "foodOrderStream";
    static String hashPrefix = "foodOrder:";
    static int howManyEntriesToWrite = 10;
    static boolean shouldCleanHashes = false;
    static int howManyEntriesToProcessEachRead = 1; // how many entries to pull from the stream in each read
    static int blockTimeForEachRead = 20000; //how long to block and wait for another entry to appear in the stream

    public static void main(String [] args){
        if(args.length>0) {
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int hostIndex = argList.indexOf("--host");
                host = argList.get(hostIndex + 1);
            }
            if (argList.contains("--port")) {
                int portIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(portIndex + 1));
            }
            if (argList.contains("--user")) {
                int userNameIndex = argList.indexOf("--user");
                username = argList.get(userNameIndex + 1);
            }
            if (argList.contains("--password")) {
                int passwordIndex = argList.indexOf("--password");
                password = argList.get(passwordIndex + 1);
            }
            if (argList.contains("--howmany")) {
                int index = argList.indexOf("--howmany");
                howManyEntriesToWrite = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--processeachread")) {
                int index = argList.indexOf("--processeachread");
                howManyEntriesToProcessEachRead = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--blocktimeeachread")) {
                int index = argList.indexOf("--blocktimeeachread");
                blockTimeForEachRead = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--cleanhashes")) {
                int index = argList.indexOf("--cleanhashes");
                shouldCleanHashes = Boolean.parseBoolean(argList.get(index + 1));
            }

        }
        HostAndPort hnp = new HostAndPort(host,port);
        System.out.println("Connecting to "+hnp.toString());

        try {
            if(!("".equalsIgnoreCase(password))){
                jedisURI = new URI("redis://" + username + ":" + password + "@" + hnp.getHost() + ":" + hnp.getPort());
            }else{
                jedisURI = new URI("redis://" + hnp.getHost() + ":" + hnp.getPort());
            }
        }catch(URISyntaxException use){use.printStackTrace();System.exit(1);}
        connectionHelper = new ConnectionHelper(jedisURI);
        testJedisConnection(jedisURI);
        if(shouldCleanHashes){
            cleanUpHashes(hashPrefix);
        }
        writeToStream(streamKeyName,howManyEntriesToWrite);
        createConsumerGroup(streamKeyName);
        consumeStreamAsWorker(streamKeyName);
    }

    static void cleanUpHashes(String keyPrefix){
        JedisPooled jedis = connectionHelper.getPooledJedis();
        String luaCleanup = "local cursor = 0 local keyNum = 0 repeat " +
                    "local res = redis.call('scan',cursor,'MATCH',KEYS[1]..'*') " +
                    "if(res ~= nil and #res>=0) then cursor = tonumber(res[1]) " +
                    "local ks = res[2] if(ks ~= nil and #ks>0) then " +
                    "for i=1,#ks,1 do " +
                    "local key = tostring(ks[i]) " +
                    "redis.call('UNLINK',key) end " +
                    "keyNum = keyNum + #ks end end until( cursor <= 0 ) return keyNum";
        jedis.eval(luaCleanup,1,keyPrefix);
    }

    static void writeToStream(String streamKeyName,int howManyEvents){
        System.out.println("Writing "+howManyEvents+" entries to the stream");
        Pipeline jedisConnection = connectionHelper.getPipeline();
        for(int x=0;x<howManyEvents;x++) {
            HashMap<String, String> orderData = FakeOrdersStreamEntryBuilder.getFakeOrderHashMap();
            orderData.put("personID", "" + Thread.currentThread().getId() + ":" + (System.nanoTime() % 30000));
            jedisConnection.xadd(streamKeyName, StreamEntryID.NEW_ENTRY, orderData);
        }
        jedisConnection.sync();
    }

    static void createConsumerGroup(String streamKeyName){
        JedisPooled jedis = connectionHelper.getPooledJedis();
        StreamEntryID streamEntryID = new StreamEntryID("0-0");
        System.out.println("StreamEntryID is " + streamEntryID);
        try{
            jedis.xgroupCreate(streamKeyName, "workers", streamEntryID, false);
            jedis.xgroupCreateConsumer(streamKeyName, "workers", "1");
        }catch(JedisDataException jde){System.out.println("Group Already Exists -- continuing on...");}
    }

    //It would be expected normally to have a separate service that kicks off the workers
    //Here we just start up a single worker - each worker gets its own ID
    static void consumeStreamAsWorker(String streamKeyName){
        JedisPooled jedis = connectionHelper.getPooledJedis();
        HashMap<String,StreamEntryID> streamEntryIDHashMap = new HashMap<>();
        streamEntryIDHashMap.put(streamKeyName,StreamEntryID.UNRECEIVED_ENTRY);
        for(int x=0;x<10;x++) {
            XReadGroupParams xReadGroupParams = XReadGroupParams.xReadGroupParams()
                    .count(howManyEntriesToProcessEachRead)
                    .block(blockTimeForEachRead);
            List<Map.Entry<String, List<StreamEntry>>> s =
                    jedis.xreadGroup("workers", "1", xReadGroupParams, streamEntryIDHashMap);
            if(null!=s) {
                Map.Entry<String, List<StreamEntry>> w = s.get(0);
                String streamKey = w.getKey();
                System.out.println("Processing up to "+howManyEntriesToProcessEachRead+" non-null streamEvents --> Events came from: " + streamKey);
                List<StreamEntry> uu = w.getValue();
                for (StreamEntry se : uu) {
                    StreamEntryID childKey = se.getID();
                    jedis.hset(hashPrefix + childKey.toString(), se.getFields());
                    jedis.xack(streamKey,"workers",childKey);
                }
            }else{
                System.out.println("No more entries to process as of this moment...  \n" +
                        "I guess I will loop around and wait "+blockTimeForEachRead+" milliseconds in case an event happens...");
            }
        }
    }


    private static void testJedisConnection(URI uri) {
        System.out.println("\ntesting jedis connection using URI == "+uri.getHost()+":"+uri.getPort());
        JedisPooled jedis = connectionHelper.getPooledJedis();
        System.out.println("Testing connection by executing 'DBSIZE' response is: "+ jedis.dbSize());
    }

}

class FakeOrdersStreamEntryBuilder{
    static String[] CIVILIAN_TIMES = new String[]{"8 AM","9 AM","10 AM","11 AM","11:30 AM","12 Noon","12:30 PM","1:00 PM","1:30 PM","2:00 PM","2:30 PM","3:00 PM","4:00 PM","5:00 PM","6:00 PM","7:00 PM","8:00 PM","8:30 PM","9:00 PM","10:00 PM"};
    static final String[] DAYS_OF_WEEK = {"Mon","Tue","Wed","Thu","Fri","Sat","Sun"};
    static final String[] APPETIZERS = {"Garlic Bread","Fried Calamari","Shrimp Cocktail","Potato Skins","Tofu Cubes","Salsa & Chips","Steamed Vegetable Dumplings"};
    static final String[] ENTREES = {"Chicken Picata","Quinoa & Beet Salad","Rib Eye & Mashed Potatoes","Eggplant Parmigiana","Linguini con Vongole","Fish & Chips","Turkey Pot Pie"};
    static final String[] DESSERTS = {"Black Forest Cake","Mochi","Banana Split","Red Jello and Cookies","French Toast Sundae","Fruit Crepe","Ginger Rice Pudding"};
    static final String[] BEVERAGES = {"Water","Ginger Ale","Beer","Red Wine","White Wine","Champagne","Mauby","Coffee","Arnold Palmer","Tea"};

    static HashMap<String,String> getFakeOrderHashMap(){
        Random random = new Random();
        int randomValue = random.nextInt(111);
        HashMap<String,String> payload = new HashMap<>();
        payload.put("delivery_time", CIVILIAN_TIMES[randomValue% CIVILIAN_TIMES.length]);
        payload.put("delivery_day",DAYS_OF_WEEK[randomValue%DAYS_OF_WEEK.length]);
        payload.put("appetizer",APPETIZERS[randomValue%APPETIZERS.length]);
        payload.put("entree",ENTREES[randomValue%ENTREES.length]);
        payload.put("dessert",DESSERTS[randomValue%DESSERTS.length]);
        payload.put("beverage",BEVERAGES[randomValue%BEVERAGES.length]);
        return payload;
    }
}

class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    public Pipeline getPipeline(){
        return new Pipeline(connectionProvider.getConnection());
    }

    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@");
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings

        }else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(30000).build(); // timeout and client settings
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}