package mx.j2.recommend.hystrix;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author DuoZhao
 * 获取是否是好友
 */
public class IsFriendHttpCommand extends HystrixCommand<String> {

    private String userId;
    private String publisherId;

    private IsFriendHttpCommand() {
        super(HystrixUtil.httpSetter);
    }

    public IsFriendHttpCommand(String userId, String publisherId) {
        this();
        this.userId = userId;
        this.publisherId = publisherId;
    }

    @Override
    protected String run() throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("uid1", userId);
        paramsMap.put("uid2", publisherId);

        return httpDataSource.get(Conf.getMxFriendListServerUrl(), paramsMap);
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
