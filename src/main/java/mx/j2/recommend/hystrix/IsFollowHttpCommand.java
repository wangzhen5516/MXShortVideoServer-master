package mx.j2.recommend.hystrix;

import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author qiqi
 * @date 2020-12-25 17:21
 */
public class IsFollowHttpCommand extends HystrixCommand<String> {

    private String userId;
    private String publisherIds;

    private IsFollowHttpCommand() {
        super(HystrixUtil.httpSetter);
    }

    public IsFollowHttpCommand(String userId, String publisherIds) {
        this();
        this.userId = userId;
        this.publisherIds = publisherIds;
    }

    @Override
    protected String run() throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("uid", userId);
        paramMap.put("publisherIds", publisherIds);
        return httpDataSource.get(Conf.getMxIsFollowedServerUrl(), paramMap);
    }

    @Override
    protected String getFallback() {
        return super.getFallback();
    }
}
