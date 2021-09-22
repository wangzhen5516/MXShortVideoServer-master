package mx.j2.recommend.hystrix;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhongrenli
 */
public class HttpServerCommand extends HystrixCommand<String> {

    private final static int FOLLOWERS_SIZE = 20;

    private BaseDataCollection baseDc;
    private String url;

    private HttpServerCommand() {
        super(HystrixUtil.httpSetter);
    }

    public HttpServerCommand(BaseDataCollection dc, String url) {
        this();
        this.baseDc = dc;
        this.url = url;
    }

    @Override
    protected String run() throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("size", String.valueOf(FOLLOWERS_SIZE));
        paramsMap.put("uid", baseDc.req.getResourceId());

        return httpDataSource.get(Conf.getMxFollowerServerUrl(), paramsMap);
    }

    @Override
    protected String getFallback() {
        Throwable e = this.getExecutionException();
        List<HystrixEventType> es = this.getExecutionEvents();

        String msg = String.format("Command key: %s, ExecutionEvents: %s, ExecutionMessage: %s, Group: %s",
                this.getCommandKey().name(), es, e, this.getCommandGroup());

        if (e != null) {
            e.printStackTrace();
        } else {
            NewRelic.noticeError(msg);
        }
        return null;
    }
}
