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
 * @Date 2021/03/18
 * @Description 根据提供的Publisher Id返回相应的Reason, POST请求
 */
public class GetFollowCardReasonHttpCommand extends HystrixCommand<String> {
    private String userId;
    private List<String> publisherIds = new ArrayList<>();

    private GetFollowCardReasonHttpCommand() {
        super(HystrixUtil.httpSetter);
    }

    public GetFollowCardReasonHttpCommand(String userId, List<String> publisherIds) {
        this();
        this.userId = userId;
        this.publisherIds.addAll(publisherIds);
    }

    @Override
    protected String run() throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        JSONObject object = new JSONObject();
        JSONArray pubIdArray = new JSONArray();

        object.put("pid", userId);
        pubIdArray.addAll(publisherIds);
        object.put("list", pubIdArray);
        object.put("with_reason", 1);

        return httpDataSource.post(Conf.getMxRemoveBlockServerUrl(), object);
    }
}

/**
 * http request documentation
 *
 * @pid : userId
 * @list : publisherIds
 * @with_reason : 0: only filter RMBlock; 1: request follow reason
 * @request_method: POST
 * POST /v2/follow/rmblocked
 * request
 * {
 * "pid": "",
 * "list": ["id1","id2"...],
 * "with_reason": 0 | 1
 * }
 * <p>
 * response
 * {
 * "pid": "",
 * "list": [
 * {
 * "user_id": "",
 * "follow_by": []
 * },
 * ...
 * ]
 * }
 **/