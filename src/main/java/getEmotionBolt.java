import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import synesketch.emotion.Emotion;
import synesketch.emotion.EmotionalState;
import synesketch.emotion.Empathyscope;
import twitter4j.Status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by amandhapola on 02/12/15.
 */
public class getEmotionBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<Integer,String> map=new HashMap<Integer,String>();
    public void setMapValue(){
        map.put(Emotion.ANGER,"anger");
        map.put(Emotion.DISGUST,"disgust");
        map.put(Emotion.FEAR,"fear");
        map.put(Emotion.HAPPINESS,"happy");
        map.put(Emotion.NEUTRAL,"neutral");
        map.put(Emotion.SADNESS,"sad");
        map.put(Emotion.SURPRISE,"surprise");
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.setMapValue();
        this.collector=outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String tweet=(String)tuple.getValueByField("res_tweet");
        Status original_tweet=(Status)tuple.getValueByField("tweet");
        try {
//            System.out.println(original_tweet.toString());
//            System.out.println(" ");
//            System.out.println("got tweet "+ tweet);
            Emotion emotion=getEmotion(tweet);
//            System.out.println(emotion.toString());
//            System.out.println(" ");
//            System.out.println(((Integer) emotion.getType()));
            String emotionClass=" ";

            if (map.containsKey((emotion.getType()))) {
                emotionClass = map.get((emotion.getType()));
                System.out.println(emotionClass);
            }
            else{
                System.out.println("key not found");
            }

//            System.out.println("emotion class :  "+ emotionClass);
//            System.out.println(" ");
            collector.emit(new Values(emotion,emotionClass,original_tweet));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("emotion","emotionCLass","original_tweet"));
    }
    Emotion getEmotion(String s) throws IOException {
        Empathyscope empathyscope = Empathyscope.getInstance();
        EmotionalState emotionalState = empathyscope.feel(s);
        Emotion emotion = emotionalState.getStrongestEmotion();
        return  emotion;
    }
}
