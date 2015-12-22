import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import synesketch.emotion.Emotion;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.User;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kritesh on 4/12/15.
 */
public class redisBolt extends BaseRichBolt {
    private static final char DELIMITER=':';
    private OutputCollector collector ;
    private Jedis jedis;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.setupRedis();
        this.setInitialValues();
    }

    @Override
    public void execute(Tuple tuple) {
        Emotion emotion = (Emotion) tuple.getValueByField("emotion");
        String emotionClass = (String) tuple.getValueByField("emotionCLass");
        Status original_tweet = (Status) tuple.getValueByField("original_tweet");
        Place place=original_tweet.getPlace();
        User user=original_tweet.getUser();
        String userName=user.getScreenName();
//        GeoLocation[][] location=null;
        boolean check=false;



//            System.out.println("null coord");
//            check=true;

        String c = jedis.get("total_count");
        HashMap <String,String> map = new HashMap<String, String>();
        HashMap <String, ArrayList<Integer>> count = new HashMap <String, ArrayList<Integer>>();
       /* try {
            System.out.println("yolo:");

        }catch (Exception e){
            System.out.println("Exeption");
        }*/
        //SET ALL VALUE TO MAP !
        map.put("text", original_tweet.getText());
        map.put("user_name", original_tweet.getUser().getName());
        map.put("user_screen_name", original_tweet.getUser().getScreenName());
        map.put("sentiment", emotionClass);
        try {
            map.put("country", original_tweet.getPlace().getCountry());

        }catch (Exception e){
            map.put("country", "");
        }

        try {
            GeoLocation[][] location = original_tweet.getPlace().getBoundingBoxCoordinates();
            map.put("lat",String.valueOf(location[0][0].getLatitude()));
            map.put("lon",String.valueOf(location[0][0].getLongitude()));
        }catch (Exception e){
            //map.put("lat:","");
            //map.put("lon","");
        }

        jedis.hmset("tweet_list:"+c, map);

        jedis.incrBy(emotionClass+"_count",1);
        jedis.incrBy("total_count", 1);

        jedis.publish("emotionClassPub",(emotionClass+DELIMITER+jedis.get(emotionClass+"_count")+DELIMITER+jedis.get("total_count")));
        twitter4j.internal.org.json.JSONObject js = new twitter4j.internal.org.json.JSONObject(map);
        if(map.get("lat")!=null && map.get("lon")!=null) {
            jedis.publish("tweets", js.toString());
        }
//        jedis.publish("total",jedis.get("total_count"));

//        System.out.println(emotionClass+":"+jedis.get(emotionClass+"_count"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    public void setupRedis() {
        jedis = new Jedis("localhost",8080);
        //DELETE ALL KEYS FIRST
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            jedis.del(key);
        }
    }
    /*SET UP INITIAL VALUES*/

    public void setInitialValues(){
        jedis.set("anger_count", "0");
        jedis.set("disgust_count", "0");
        jedis.set("fear_count", "0");
        jedis.set("happy_count", "0");
        jedis.set("neutral_count", "0");
        jedis.set("sad_count", "0");
        jedis.set("surprise_count", "0");
        jedis.set("total_count","0");
    }
}
