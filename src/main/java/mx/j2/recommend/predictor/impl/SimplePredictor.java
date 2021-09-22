package mx.j2.recommend.predictor.impl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;


/**
 * Simple预测器
 *
 * @author zhangxuejian
 */
public class SimplePredictor extends BasePredictor<BaseDataCollection> {

    static String[][] input =
            new String[][]{{"0","0","0","","-1","20","tiktok_trend","no_language","19245","0.022","19245","0.022","11.932","0.8694","0.3473","0.0","0.0135","1.0E-4","3.0E-4","19018","0.0224","19018","0.0224","11.8999","0.8686","0.3462","0.0","0.0136","1.0E-4","3.0E-4","5872","0.0192","5872","0.0192","11.4189","0.8498","0.3312","0.0","0.0186","0.0","2.0E-4","8670154","0.6503","0.0465","0.4752","0.0194","0.0013","7.0E-4","94581","368","16.0","1.0","15.3264","","","0.0","0.6349","1.0"}};

    BasicAWSCredentials credential = new BasicAWSCredentials(
            Conf.getAwsAccessKeyId(), Conf.getAwsSecretAccessKey());

    AmazonSageMakerRuntime client = AmazonSageMakerRuntimeClientBuilder
            .standard()
            .withRegion(Regions.AP_SOUTH_1)
            .withCredentials(new AWSStaticCredentialsProvider(credential))
            .build();

    @Override
    public void predict(BaseDataCollection dc) {

        InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest();
        invokeEndpointRequest.setContentType("text/csv");

        String data = Arrays.toString(input);
        //data = "0,0,0,,-1,20,tiktok_trend,no_language,19245,0.022,19245,0.022,11.932,0.8694,0.3473,0.0,0.0135,1.0,3.0,19018,0.0224,19018,0.0224,11.8999,0.8686,0.3462,0.0,0.0136,1.0,3.0,5872,0.0192,5872,0.0192,11.4189,0.8498,0.3312,0.0,0.0186,0.0,2.0,8670154,0.6503,0.0465,0.4752,0.0194,0.0013,7.0,94581,368,16.0,1.0,15.3264,,,0.0,0.6349,1.0";
        data = "0,0,0,,-1,20,tiktok_trend,no_language,19245,0.022,19245,0.022,11.932,0.8694,0.3473,0.0,0.0135,1.0,3.0,19018,0.0224,19018,0.0224,11.8999,0.8686,0.3462,0.0,0.0136,1.0,3.0,5872,0.0192,5872,0.0192,11.4189,0.8498,0.3312,0.0,0.0186,0.0,2.0,8670154,0.6503,0.0465,0.4752,0.0194,0.0013,7.0,94581,368,16.0,1.0,15.3264,,,0.0,0.6349,";

        Random random = new Random();
        String dataSet = data + random.nextDouble();
        int predictNum = Integer.parseInt(null == Conf.getDebugInfo("predictNum") ? "10" : Conf.getDebugInfo("predictNum"));
        for (int i = 1; i <= predictNum; i++) {
            dataSet += "\n" + data + random.nextDouble();
        }
        System.out.println(dataSet);

        try {
            invokeEndpointRequest.setBody(ByteBuffer.wrap(dataSet.getBytes("UTF-8")));
        } catch (java.io.UnsupportedEncodingException e) {
        }
        invokeEndpointRequest.setEndpointName("taka-20201207-train-v11-1-model");

        InvokeEndpointResult result = client.invokeEndpoint(invokeEndpointRequest);
        String body = StandardCharsets.UTF_8.decode(result.getBody()).toString();
        System.out.println("\n\n--------- Raw Results ----------");
        System.out.println(body);

        System.out.println("\n\n--------- Parsed Results ----------");
    }

}
