package mx.j2.recommend.hystrix;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class GetPublisherHistoryCommand extends HystrixCommand<Boolean> {
    private static Logger logger = LogManager.getLogger(GetPublisherHistoryCommand.class);

    private String userId;
    private List<Result> resultList;

    public GetPublisherHistoryCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetPublisherHistoryCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("guh-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(200)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(2000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    public GetPublisherHistoryCommand(String userId, List<Result> resultList) {
        this();
        this.userId = userId;
        this.resultList = resultList;
    }

    @Override
    protected Boolean run() throws Exception {
        AmazonSNSAsyncClient client = MXDataSource.SNS().getAmazonSNSAsyncClient();

        JSONObject object = new JSONObject();
        List<String> publisherId = new ArrayList<>();
        for(Result result: resultList){
            if (null != result.internalUse && MXStringUtils.isNotEmpty(result.internalUse.getPublisherId()) && result.internalUse.isBigV) {
                publisherId.add(result.getInternalUse().getPublisherId());
            }
        }

        object.put("userId", userId);
        object.put("publisherId", publisherId);
        object.put("timestamp", System.currentTimeMillis());

        PublishRequest request = new PublishRequest(Conf.getTopicArnPublisher(), object.toJSONString());
        client.publishAsync(request);
        return true;
    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "GetPublisherHistoryCommand resultList abnormal. logId", "nullId");
        return false;
    }
}
