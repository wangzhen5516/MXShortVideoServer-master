package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author DuoZhao
 * 召回主页的Private视频，必须是本人
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 下午4:05 2020/09/17
 * @ Description：${description}
 */
public class VideosOfThePublisherPrivateRecall extends SearchEngineRecall<OtherDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfThePublisherRecall.class);
    private String requestUrlFormat = "";
    private JSONArray sortJson;
    private final static int RECALL_SIZE = 40;
    private final static int RECALL_FROM = 0;

    public final static int MAX_DEFAULT = 100000;
    private final static String INDEX_FORMAT = "takatak_simple_trigger_v%s";

    private final static String SORT_FIELD = "online_time";

    /**
     * 构造函数
     */
    public VideosOfThePublisherPrivateRecall() {
        init();
    }

    /**
     * 初始化
     */
    @Override
    public void init() {
        requestUrlFormat = "/%s/_search?pretty=false";

        sortJson = new JSONArray();

        JSONObject sortCore0 = new JSONObject();
        sortCore0.put("order", "asc");
        sortCore0.put("missing", MAX_DEFAULT);
        JSONObject sortObj0 = new JSONObject();
        sortObj0.put("order", sortCore0);
        sortJson.add(sortObj0);

        JSONObject sortCore1 = new JSONObject();
        sortCore1.put("order", "desc");
        sortCore1.put("missing", "0");
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put("is_ugc_content", sortCore1);
        sortJson.add(sortObj1);

        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "desc");
        sortCore2.put("missing", "0");
        JSONObject sortObj2 = new JSONObject();
        sortObj2.put(SORT_FIELD, sortCore2);
        sortJson.add(sortObj2);

        JSONObject sortCore3 = new JSONObject();
        sortCore3.put("order", "desc");
        sortCore3.put("missing", "0");
        JSONObject sortObj3 = new JSONObject();
        sortObj3.put("_id", sortCore3);
        sortJson.add(sortObj3);

    }

    @Override
    public boolean skip(OtherDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.getResourceId())) {
            return true;
        }
        if (MXJudgeUtils.isEmpty(dc.req.getResourceType())) {
            return true;
        }
        if (!BloomUtil.getUserId(dc).equals(dc.req.resourceId)) {
            return true;
        }

        return false;
    }

    @Override
    public void recall(OtherDataCollection dc) {
        constructRequestURL(dc);
    }

    @Override
    @Trace(dispatcher = true)
    public void constructRequestURL(OtherDataCollection baseDc) {
        int hash = baseDc.req.getResourceId().hashCode() & Integer.MAX_VALUE;
        String indexUrl = String.format(INDEX_FORMAT, hash % 16);
        String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);

        Map<String, String> condition = new HashMap<>();
        condition.put(baseDc.req.getResourceType() + "_id", baseDc.req.getResourceId());
        assembleRequest(baseDc, elasticSearchRequest, condition);

        baseDc.searchEngineRecallerSet.add(this.getName());
    }

    /**
     * 填充dc中相应的请求内容
     */
    private void assembleRequest(OtherDataCollection dc, String elasticSearchRequest, Map<String, String> conditionMap) {
        JSONObject query = constructQueryByConditionAndPrivate(conditionMap);

        if (MXJudgeUtils.isEmpty(query)) {
            return;
        }

        JSONObject content = constructContent(query, RECALL_FROM, RECALL_SIZE, null, sortJson);

        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken)) {
            JSONArray sort = parseNextToken(dc.req.nextToken);
            if (null != sort) {
                content.put("search_after", sort);
            }
        }
        String request = content.toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("VideosOfThePublisherRecall search url : %s", request));
            log.debug(String.format("VideosOfThePublisherRecall search url : %s", elasticSearchRequest));
        }

        dc.addToESRequestList(
                elasticSearchRequest,
                request,
                this.getName(), "",
                DefineTool.EsType.VIDEO_NEW.getTypeName());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    // 为了添加view_privacy条件
    private JSONObject constructQueryByConditionAndPrivate(Map<String, String> conditionMap) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        JSONObject specialMatchFather = new JSONObject();
        JSONObject specialMatch = new JSONObject();
        specialMatch.put("view_privacy", 2);
        specialMatchFather.put("match", specialMatch);
        must.add(specialMatchFather);

        JSONObject isDeleteFather = new JSONObject();
        JSONObject isDelete = new JSONObject();
        isDelete.put("is_delete", true);
        isDeleteFather.put("match", isDelete);

        mustNot.add(isDeleteFather);

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(nextToken, "|");
        if (3 > tokens.length) {
            return null;
        }

        int token_1 = Integer.parseInt(tokens[0]);
        int token_2 = Integer.parseInt(tokens[1]);
        long token_3 = Long.parseLong(tokens[2]);
        String token_4 = tokens[3];
        result.add(token_1);
        result.add(token_2);
        result.add(token_3);
        result.add(token_4);

        return result;
    }

}