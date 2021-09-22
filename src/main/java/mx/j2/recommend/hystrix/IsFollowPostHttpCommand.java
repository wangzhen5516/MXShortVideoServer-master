package mx.j2.recommend.hystrix;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.netflix.hystrix.HystrixCommand;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.HystrixUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author DuoZhao
 * @Date 2021/03/22
 * @Description 判断publisher id是否已经follow的接口，单次最多查询200个
 */
public class IsFollowPostHttpCommand extends HystrixCommand<String> {
    private String userId;
    private List<String> publisherIds = new ArrayList<>();

    private IsFollowPostHttpCommand() {
        super(HystrixUtil.httpSetter);
    }

    public IsFollowPostHttpCommand(String userId, List<String> publisherIds) {
        this();
        this.userId = userId;
        this.publisherIds.addAll(publisherIds);
    }

    @Override
    protected String run() throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        JSONObject object = new JSONObject();
        JSONArray pubIdArray = new JSONArray();

        object.put("uid", userId);
        pubIdArray.addAll(publisherIds);
        object.put("publisherList", pubIdArray);
        return httpDataSource.post(Conf.getMxIsFollowedServerUrl(), object);
    }
}
