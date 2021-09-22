package mx.j2.recommend.data_source;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeAsync;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeAsyncClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.SegaMakerCommand;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:59 下午 2020/12/18
 */
public class AmazonSageDataSource extends BaseDataSource{

    private AmazonSageMakerRuntimeAsync client;

    public AmazonSageDataSource() {
        init();
    }

    private void init() {
        if (null == client) {
            initClient();
        }
    }

    private void initClient() {
        BasicAWSCredentials credential = new BasicAWSCredentials(
                Conf.getAwsAccessKeyId(), Conf.getAwsSecretAccessKey());
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setClientExecutionTimeout(500);
        configuration.setConnectionTimeout(500);
        client = AmazonSageMakerRuntimeAsyncClientBuilder
                .standard()
                .withClientConfiguration(configuration)
                .withRegion(Regions.AP_SOUTH_1)
                .withCredentials(new AWSStaticCredentialsProvider(credential))
                .build();

    }

    public AmazonSageMakerRuntimeAsync getClient() {
        if (null == client) {
            initClient();
        }
        return client;
    }

    public String sendInAsync(BaseDataCollection dc, String endpointName) {
        InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest();
        invokeEndpointRequest.setContentType("text/csv");

        invokeEndpointRequest.setBody(ByteBuffer.wrap(dc.featureString.getBytes(StandardCharsets.UTF_8)));
        invokeEndpointRequest.setEndpointName(endpointName);

        SegaMakerCommand command = new SegaMakerCommand(dc, client, invokeEndpointRequest);
        return command.execute();
    }
}
