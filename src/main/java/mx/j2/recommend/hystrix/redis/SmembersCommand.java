package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author qiqi
 * @date 2021-04-26 17:33
 */
public class SmembersCommand extends HystrixCommand<List<String>> {

    private static Logger logger = LogManager.getLogger(SmembersCommand.class);

    private String key;


    public SmembersCommand(String key) {
        this();
        this.key = key;
    }

    private SmembersCommand() {
        super(HystrixCommand.Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SmembersCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("smembers-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(800)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(2000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    @Trace(dispatcher = true)
    @Override
    public List<String> run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getStrategyConnection();
        RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();
        if (commands == null) {
            return null;
        }
        RedisFuture<Set<String>> items = commands.smembers(key);
        if (items == null || items.get() == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(items.get());
    }

    @Override
    protected List<String> getFallback() {
        HystrixUtil.logFallback(this);
        return Collections.emptyList();
    }
}


