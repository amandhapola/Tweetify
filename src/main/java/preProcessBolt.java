import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by amandhapola on 30/11/15.
 */
public class preProcessBolt extends BaseRichBolt{
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        String tweet_text=tweet.getText();
        String res_tweet=preProcess(tweet_text);
        collector.emit(new Values(res_tweet,tweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("res_tweet","tweet"));
    }
    public String preProcess(String s){
        String t=s.replaceAll("http[^\\s]+","");
        int split_index=0;
        String res=t;
        if (t.indexOf("RT") != -1) {
            split_index = t.indexOf(':');
            res =t.substring(split_index+1);
            return res;
        }
        else{
            return res;
        }

    }
}
