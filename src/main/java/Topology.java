import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.IOException;

class Topology{
    static final String TOPOLOGY_NAME = "twitter_topology";
    public static void main(String[] args) throws IOException {
//        Empathyscope empathyscope = Empathyscope.getInstance();
//        EmotionalState emotionalState = empathyscope.feel("hello threre i m feeling happy");
//        Emotion emotion = emotionalState.getStrongestEmotion();
//        HashMap<Integer,String> map;
//        map=new HashMap<Integer,String>();
//        map.put(Emotion.ANGER,"Anger");
//        map.put(Emotion.DISGUST,"Disgust");
//        map.put(Emotion.FEAR,"Fear");
//        map.put(Emotion.HAPPINESS,"HAPPINESS");
//        map.put(Emotion.NEUTRAL,"Neutral");
//        map.put(Emotion.SADNESS,"sad");
//        map.put(Emotion.SURPRISE,"Surprise");
//        String emotionClass="";
//        if (map.containsKey(emotion.getType())) {
//            emotionClass = map.get((emotion.getType()));
//        }
//        System.out.println(emotionClass);


        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSpout", new twitterSpout());
        b.setBolt("preProcessBolt",new preProcessBolt(),5).shuffleGrouping("TwitterSpout");
        b.setBolt("getEmotionBolt",new getEmotionBolt(),5).shuffleGrouping("preProcessBolt");
        b.setBolt("redisBolt", new redisBolt(),5).fieldsGrouping("getEmotionBolt", new Fields("emotionCLass"));
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });

    }

}