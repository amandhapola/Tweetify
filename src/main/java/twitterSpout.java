import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings({ "rawtypes", "serial" })
class twitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
//    public EmotionalState emotionalState;
    private TwitterStream twitterStream;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        queue = new LinkedBlockingQueue<Status>();

        this.collector=collector;
        StatusListener listener=new StatusListener(){
            public void onStatus(Status status){
                queue.offer(status);
//                    System.out.println(" ");
//                System.out.println(status);
//                    System.out.println(status.getUser().getName()+" : "+status.getText());
//                    System.out.println(" ");
            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice){}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses){}
            public void onException(Exception ex){
                ex.printStackTrace();
            }
        };
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("8PkdXIhwYZMaSnAJv5OZvYkmM")
                .setOAuthConsumerSecret("r5CUub3pBpHuQ4VwpJ2Ys5j022OpVtKbVpB3qfZ8EqVwu8YMG0")
                .setOAuthAccessToken("2901834871-iINAPDY7YAW5vGBL6KROlvXQubJcX9TdxmOdEct")
                .setOAuthAccessTokenSecret("56b3RR3PLJwKgmvW6QviaE3pt6sDGi5GSf1PkcZ7YNYYQ");
        twitterStream=new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        //adding filter
        FilterQuery filterQuery=new FilterQuery();
        String[] keywords={"india","narendra modi"};
        filterQuery.track(keywords);
        twitterStream.filter(filterQuery);
//        twitterStream.sample();
    }
    @Override
    public void nextTuple(){
        Status tweet = queue.poll();
        if (tweet==null){
            Utils.sleep(50);
        }else {
            collector.emit(new Values(tweet));
        }
    }

    @Override
    public void deactivate() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void close(){
        twitterStream.shutdown();
    }
    @Override
    public void ack(Object id)
    {

    }
    @Override
    public void fail(Object id) {
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("tweet"));
    }
}
