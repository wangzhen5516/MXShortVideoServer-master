package mx.j2.recommend.hystrix.es;

import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.util.HystrixUtil;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.util.Collections;

public class ESRequestCommand extends HystrixCommand<String> {
    private String request;
    private String content;
    private RestClient restClient;

    private ESRequestCommand() {
        super(HystrixUtil.ES_SETTER);
    }

    public ESRequestCommand(RestClient restClient, String request, String content) {
        this();
        this.request = request;
        this.content = content;
        this.restClient = restClient;
    }

    @Override
    protected String run() throws Exception {
        HttpEntity body = new NStringEntity(content, ContentType.APPLICATION_JSON);

        Response response = restClient.performRequest(
                "POST",
                request,
                Collections.emptyMap(),
                body);

        return EntityUtils.toString(response.getEntity());
    }

    @Override
    protected String getFallback() {
        HystrixUtil.logFallback(this);
        return null;
    }
}
