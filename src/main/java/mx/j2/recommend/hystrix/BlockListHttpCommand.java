package mx.j2.recommend.hystrix;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;
import mx.j2.recommend.util.MXStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author DuoZhao
 * @Date 2020/11/12 15:06
 * 获取是否是拉黑关系
 */
public class BlockListHttpCommand extends HystrixCommand<String> {

    private String userId;
    private String publisherId;

    private BlockListHttpCommand() {
        super(HystrixUtil.httpSetter);
    }

    public BlockListHttpCommand(String userId) {
        this();
        this.userId = userId;
    }

    @Override
    protected String run() throws Exception {
        if (MXStringUtils.isEmpty(userId)) {
            return null;
        }
        HttpDataSource httpDataSource = MXDataSource.http();
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("uid", userId);

        return httpDataSource.get(Conf.getMxIsBlockServerUrl(), paramsMap);
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
