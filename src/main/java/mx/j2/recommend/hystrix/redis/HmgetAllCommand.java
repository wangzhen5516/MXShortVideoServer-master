package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.HystrixCommand;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @author zhongrenli
 */
public class HmgetAllCommand extends HystrixCommand<Map<String, String>> {
    private static final Logger logger = LogManager.getLogger(HmgetAllCommand.class);
    private final String redisKey;
    private final Supplier<StatefulRedisClusterConnection<String, String>> supplier;

    public HmgetAllCommand(Setter setter, String key, Supplier<StatefulRedisClusterConnection<String, String>> supplier) {
        super(setter);
        this.redisKey = key;
        this.supplier = supplier;
    }

    @Override
    public Map<String, String> run() {
        try{
            StatefulRedisClusterConnection<String, String> connection = this.supplier.get();
            RedisAdvancedClusterAsyncCommands<String, String> command = connection.async();
            RedisFuture<Map<String, String>> future = command.hgetall(this.redisKey);
            return future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        }catch (InterruptedException|ExecutionException|TimeoutException e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Map<String, String> getFallback() {
        this.getFailedExecutionException().printStackTrace();
        LogTool.printJsonStatusLog(logger, "HmgetAllCommand redis abnormal. ", this.supplier.toString());
        return null;
    }
}
