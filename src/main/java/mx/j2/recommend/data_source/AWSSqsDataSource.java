package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author qiqi
 * @date 2020-12-21 15:20
 */
public class AWSSqsDataSource {

    Logger logger = LogManager.getLogger(AWSSqsDataSource.class);
    /**
     * publisher_id
     */
    private static final String PUB_ID = "pub_id";
    /**
     * 时间戳
     */
    private static final String TIME_STAMP = "timestamp";
    /**
     * 订阅url
     */
    private String topicSqsUrl;
    private AmazonSQSClient amazonSqsClient;

    public AWSSqsDataSource() {
        topicSqsUrl = Conf.getVideoNumSqsUrl();
        initAwsSqsDataAsyncClient();
    }

    private void initAwsSqsDataAsyncClient() {
        BasicAWSCredentials credential = new BasicAWSCredentials(Conf.getAwsAccessKeyId(), Conf.getAwsSecretAccessKey());
        try {
            amazonSqsClient = new AmazonSQSAsyncClient(credential).withRegion(Regions.AP_SOUTH_1);
        } catch (Exception e) {
            logger.error("initAwsSqsDataAsyncClient is error", e);
        }
    }

    public void sendMessage(String publisherId) {
        if (MXStringUtils.isBlank(publisherId)) {
            return;
        }
        JSONObject object = new JSONObject();
        object.put(PUB_ID, publisherId);
        object.put(TIME_STAMP, System.currentTimeMillis());
        SendMessageRequest request = new SendMessageRequest(topicSqsUrl, object.toJSONString());
        try {
            amazonSqsClient.sendMessage(request);
        } catch (Exception e) {
            logger.error(String.format("sendMessage is error, id:%s", publisherId), e);
        }
    }

    public static void main(String[] args) {
        AWSSqsDataSource awsSqsDataSource = new AWSSqsDataSource();
        awsSqsDataSource.sendMessage("12107283972751083301885");
        System.out.println("send done");
    }
}
