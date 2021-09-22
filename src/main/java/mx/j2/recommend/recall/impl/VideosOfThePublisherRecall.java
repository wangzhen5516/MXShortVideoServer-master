package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2018/12/5
 * @ Description：${description}
 * @author zhongrenli
 */
@Deprecated
public class VideosOfThePublisherRecall extends SearchEngineRecall<OtherDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfThePublisherRecall.class);
    private String requestUrlFormat = "";
    private JSONArray sortJson;
    private final static int RECALL_SIZE = DefineTool.Recall.Config.SizeEnum.VIDEOS_OF_PUBLISHER.configValue;
    private final static int RECALL_FROM = 0;

    public final static int MAX_DEFAULT = 100000;

    private final static String SORT_FIELD = "online_time";

    private final static DefineTool.CategoryEnum CATEGORY_ENUM = DefineTool.CategoryEnum.SHORT_VIDEO;

    private final static String isHaveTopHotInPublisher = "{\"size\":1,\"query\":{\"bool\":{\"must\":[{\"match\":{\"publisher_id\":\"%s\"}},{\"exists\":{\"field\":\"order\"}}],\"must_not\":[{\"match\":{\"order\":100000}}]}}}";


    /**
     * 构造函数
     */
    public VideosOfThePublisherRecall() {
        init();
    }

    /**
     *
     * 初始化
     *
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
        sortObj3.put("_uid", sortCore3);
        sortJson.add(sortObj3);

    }

    @Override
    public boolean skip(OtherDataCollection data) {
        return false;
    }

    @Override
    public void recall(OtherDataCollection dc) {
        constructRequestURL(dc);
    }

    protected boolean isHaveTopHotInPublisher(OtherDataCollection dc) {
        String publisherId = dc.req.getResourceId();
        if(MXStringUtils.isBlank(publisherId)) {
            return false;
        }
        // get from local
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        Boolean isHaveTop = localCacheDataSource.getCacheIsHaveTopHotInPublisher(publisherId);
        if(null != isHaveTop) {
            return isHaveTop;
        }
        // get from ES
        String indexUrl = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
        String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);

        String request = String.format(requestUrlFormat, indexUrl);
        String context = String.format(isHaveTopHotInPublisher, dc.req.getResourceId() );


        int total = MXDataSource.videoES().sendSyncOnlyReturnTotal(request, context);
        boolean isHave = false;
        if (total > 0) {
            isHave = true;
        }
        // set local
        localCacheDataSource.setCacheIsHaveTopHotInPublisher(publisherId, isHave);

        return isHave;
    }

    @Override
    @Trace(dispatcher = true)
    public void constructRequestURL(OtherDataCollection baseDc) {
        if (MXJudgeUtils.isEmpty(baseDc.req.getResourceId())) {
            return;
        }

        if (MXJudgeUtils.isEmpty(baseDc.req.getResourceType())) {
            return;
        }

        String indexUrl = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
        String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);

        Map<String, String> condition = new HashMap<>();
        condition.put(baseDc.req.getResourceType()+"_id", baseDc.req.getResourceId());
        assembleRequest(baseDc, elasticSearchRequest, condition);

        baseDc.searchEngineRecallerSet.add(this.getName());
    }

    /**
     *
     * 填充dc中相应的请求内容
     *
     */
    private void assembleRequest(OtherDataCollection dc, String elasticSearchRequest, Map<String, String> conditionMap) {
        JSONObject query = constructQueryByConditionFilterPrivateVideo(conditionMap);

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
                DefineTool.EsType.VIDEO.getTypeName()
        );
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(nextToken,"|");
        if (3 > tokens.length) {
            return null;
        }

        int token_1 = Integer.parseInt(tokens[0]);
        long token_2 = Long.parseLong(tokens[1]);
        String token_3 = tokens[2];
        //TODO: 默认token_4是不会小于0的。
        int token_4 = -1;
        //兼容老板本
        if(tokens.length >= 4) {
            token_4 = Integer.parseInt(tokens[3]);
            result.add(token_4);
        }
        result.add(token_1);
        result.add(token_2);
        result.add(token_3);

        return result;
    }

    public static void main(String[] args) throws IOException {
        OtherDataCollection dc = new OtherDataCollection();
        dc.req = new Request();
        dc.req.resourceId = "111";
        dc.req.resourceType = "publisher";
        new VideosOfThePublisherRecall().recall(dc);
    }
}