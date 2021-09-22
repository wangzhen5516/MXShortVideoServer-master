package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.GetPublisherHistoryCommand;
import mx.j2.recommend.util.MXJudgeUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-22
 */
public class AWSSnsDataSource {

    /**
     * GetUserNewHistoryCommand
     */
    private String topicArn;

    /**
     * AWS-SNS服务Client
     */
    private AmazonSNSAsyncClient amazonSNSAsyncClient;

    public AWSSnsDataSource() {
        init();

    }

    private void init() {
        topicArn = Conf.getTopicArn();

        initAmazonSNSAsyncClient();
    }

    private void initAmazonSNSAsyncClient(){
        BasicAWSCredentials credential = new BasicAWSCredentials(Conf.getAwsAccessKeyId(), Conf.getAwsSecretAccessKey());
        try{
            amazonSNSAsyncClient = new AmazonSNSAsyncClient(credential).withRegion(Regions.AP_SOUTH_1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public AmazonSNSAsyncClient getAmazonSNSAsyncClient() {
        if (null == amazonSNSAsyncClient) {
            initAmazonSNSAsyncClient();
        }
        return amazonSNSAsyncClient;
    }

    public void send(String userId) {
        JSONObject object = new JSONObject();
        object.put("userId", userId);
        object.put("timestamp", System.currentTimeMillis());
        try {
            object.put("hostname", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            object.put("hostname", "");
        }
        PublishRequest request = new PublishRequest(topicArn, object.toJSONString());
        try{
            amazonSNSAsyncClient.publishAsync(request);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Trace(dispatcher = true)
    public void getPublisherHistory(BaseDataCollection dc){
        if(dc.data.response == null){
            return;
        }
        if(MXJudgeUtils.isEmpty(dc.data.response.resultList)) {
            return;
        }
        try {
            HystrixCommand<Boolean> command = new GetPublisherHistoryCommand(dc.client.user.userId, dc.data.response.resultList);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Conf.loadConf("./conf/conf.sample.properties");
        new AWSSnsDataSource().send("12106971881147830726211");
    }
}

