import backtype.storm.tuple.Tuple;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

/**
 * Created by amandhapola on 03/12/15.
 */
public class redisStoreBolt implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public  redisStoreBolt(){
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return  description;
    }

    @Override
    public String getKeyFromTuple(backtype.storm.tuple.ITuple iTuple) {
        return null;
    }

    @Override
    public String getValueFromTuple(backtype.storm.tuple.ITuple iTuple) {
        return null;
    }
}
