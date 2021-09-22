package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/1/9 下午5:26
 * @description
 */
public class HotestHashTag15DataSource extends BaseDataSource {

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final JSONArray SORT = JSON.parseArray("[{'order':{'order':'desc'}}]");

    private List<String> hotestCardNameList = new ArrayList<>();

    public HotestHashTag15DataSource() {
        init();
    }

    public void init() {
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        scheduledExecutorService.scheduleAtFixedRate(this::getHotestHashTag, 0, 30, TimeUnit.SECONDS);
    }

    public void getHotestHashTag() {
        List<String> cardIdList = new ArrayList<>();
        List<String> cardNameList = new ArrayList<>();

        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();

        //查询id的index和content
        String indexId = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.CARDLISTITEM.getIndexAndType());
        String contentId = constructContentOfId().toString();
        List<JSONObject> resultId = elasticSearchDataSource.sendSyncSearchPure(indexId, contentId);

        if (MXJudgeUtils.isEmpty(resultId)) {
            return;
        }
        //获取前15个cardId
        for (JSONObject obj : resultId) {
            if (obj != null) {
                cardIdList.add(obj.getString("card_id"));
            }
        }

        //查询name的index和content
        String indexName = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.CARDNEW.getIndexAndType());
        String contentName = constructContentOfName(cardIdList).toString();
        List<JSONObject> resultName = elasticSearchDataSource.sendSyncSearchPure(indexName, contentName);
        if (MXJudgeUtils.isEmpty(resultName)) {
            return;
        }
        for (JSONObject obj : resultName) {
            if (obj != null) {
                cardNameList.add(obj.getString("hashtag_id"));
            }
        }

        hotestCardNameList = cardNameList;
    }

    public JSONObject constructContentOfId() {
        JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");
        JSONObject content = new JSONObject();

        //写死成1,表示已经上线
        musts.add(getMatch("status", "1"));
        musts.add(getMatch("platform_type", "1"));

        content.put("query", query);
        content.put("size", 15);
        content.put("sort", SORT);

        return content;
    }

    public JSONObject constructContentOfName(List<String> idList) {
        JSONObject content = new JSONObject();
        JSONObject query = JSON.parseObject("{'bool':{'should':[]}}");
        JSONArray should = query.getJSONObject("bool").getJSONArray("should");

        for (String id : idList) {
            should.add(getMatch("_id", id));
        }

        content.put("query", query);
        content.put("size", idList.size());
        return content;
    }

    public JSONObject getMatch(String key, String value) {
        String ret = String.format("{'match':{'%s': '%s'}}", key, value);
        return JSON.parseObject(ret);
    }

    public boolean isHotest(String cardName) {
        return hotestCardNameList.contains(cardName);
    }
}
