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
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false  -Dexec.args="--howmany 100 --host 192.168.1.1 -port 12000 --username gerard --password &pTRsFE$E"
 *  *
 */
public class Main {
    static String host = "localhost";
    static int port = 6379;
    static String username = "default";
    static String password = "";
    static URI jedisURI = null;
    static ConnectionHelper connectionHelper = null;
    static String streamKeyName = "foodOrderStream";
    static int howManyEntriesToWrite = 10;


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
            if (argList.contains("--username")) {
                int userNameIndex = argList.indexOf("--username");
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
        writeToStream(streamKeyName,howManyEntriesToWrite);
        createConsumerGroup(streamKeyName);
        consumeStreamAsWorker(streamKeyName);
    }

    static void writeToStream(String streamKeyName,int howManyEvents){
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

    static void consumeStreamAsWorker(String streamKeyName){
        JedisPooled jedis = connectionHelper.getPooledJedis();
        HashMap<String,StreamEntryID> streamEntryIDHashMap = new HashMap<>();
        streamEntryIDHashMap.put(streamKeyName,StreamEntryID.UNRECEIVED_ENTRY);
        for(int x=0;x<10;x++) {
            List<Map.Entry<String, List<StreamEntry>>> s = jedis.xreadGroup("workers", "1", XReadGroupParams.xReadGroupParams(), streamEntryIDHashMap);
            if(null!=s) {
                Map.Entry<String, List<StreamEntry>> w = s.get(0);
                String streamKey = w.getKey();
                System.out.println("while processing streamEvents --> thing called streamKey == " + streamKey);
                List<StreamEntry> uu = w.getValue();
                for (StreamEntry se : uu) {
                    StreamEntryID childKey = se.getID();
                    jedis.hset("foodOrder:" + childKey.toString(), se.getFields());
                }
            }else{
                System.out.println("All entries processed as of this moment...");
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
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().connectionTimeoutMillis(30000).build(); // timeout and client settings
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
