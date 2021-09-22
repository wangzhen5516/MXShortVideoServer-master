package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.HystrixCommand;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-12-28
 */

public class HMgetForClusterCommand extends HystrixCommand<Map<String, String>> {
    private static final Logger logger = LogManager.getLogger(HMgetForClusterCommand.class);
    private String redisKey;
    private List<String> fields;
    private Supplier<StatefulRedisClusterConnection<String, String>> supplier;

    public HMgetForClusterCommand(Setter setter, String key, List<String> fields, Supplier<StatefulRedisClusterConnection<String, String>> supplier) {
        super(setter);
        if(MXStringUtils.isEmpty(key)){
            throw new IllegalArgumentException("Key Must Not Be empty.");
        }
        if(MXJudgeUtils.isEmpty(fields)){
            throw new IllegalArgumentException("Fields Must Not Be Empty.");
        }
        this.redisKey = key;
        this.fields = fields;
        this.supplier = supplier;
    }

    @Override
    //map value may be null
    public Map<String, String> run() {
        try{
            StatefulRedisClusterConnection<String, String> connection = this.supplier.get();
            RedisAdvancedClusterAsyncCommands<String, String> command = connection.async();
            RedisFuture<List<KeyValue<String, String>>> future = command.hmget(this.redisKey, this.fields.toArray(new String[0]));
            Map<String, String> result = new HashMap<>(this.fields.size());
            List<KeyValue<String, String>> redisResult = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            for (int i = 0; i < redisResult.size(); i++) {
                result.put(redisResult.get(i).getKey(), redisResult.get(i).getValueOrElse(null));
            }
            return result;
        }catch (InterruptedException|ExecutionException|TimeoutException e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Map<String, String> getFallback() {
        this.getFailedExecutionException().printStackTrace();
        LogTool.printJsonStatusLog(logger, "HMgetForClusterCommand redis abnormal. ", this.supplier.toString());
        return null;
    }
}
