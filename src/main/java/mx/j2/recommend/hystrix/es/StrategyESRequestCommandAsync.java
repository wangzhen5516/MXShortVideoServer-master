package mx.j2.recommend.hystrix.es;

import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.util.Collections;

public class StrategyESRequestCommandAsync extends HystrixCommand<Boolean> {
    private String request;
    private String content;
    private RestClient restClient;
    private ResponseListener responseListener;

    private StrategyESRequestCommandAsync() {
        super(HystrixUtil.strategyEsSetter);
    }

    public StrategyESRequestCommandAsync(RestClient restClient, String request, String content, ResponseListener responseListener) {
        this();
        this.request = request;
        this.content = content;
        this.restClient = restClient;
        this.responseListener = responseListener;
    }

    @Override
    protected Boolean run() {
        HttpEntity body = new NStringEntity(content, ContentType.APPLICATION_JSON);

        restClient.performRequestAsync(
                "POST",
                request,
                Collections.emptyMap(),
                body,
                responseListener);

        return true;
    }

    @Override
    protected Boolean getFallback() {
        HystrixUtil.logFallback(this);
        return Boolean.FALSE;
    }
}
