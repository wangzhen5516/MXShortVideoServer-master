package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_source.PublisherPageWhiteDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2020/10/21
 * @ Description：${description}
 */
public class VideosOfThePublisherNewRecall extends SearchEngineRecall<OtherDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfThePublisherNewRecall.class);
    private String requestUrlFormat = "";
    private JSONArray sortJson;
    private final static int RECALL_SIZE = DefineTool.Recall.Config.SizeEnum.VIDEOS_OF_PUBLISHER.configValue;
    private final static int RECALL_FROM = 0;

    public final static int MAX_DEFAULT = 100000;

    private final static String SORT_FIELD = "online_time";

    private final static String INDEX_FORMAT = "takatak_simple_trigger_v%s";

    private final static DefineTool.CategoryEnum CATEGORY_ENUM = DefineTool.CategoryEnum.SHORT_VIDEO;
    private final static String INTERFACE_NAME = "mx_videos_of_the_publisher_me_version_1_0";

    /**
     * 构造函数
     */
    public VideosOfThePublisherNewRecall() {
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
        sortCore0.put("order", "desc");
        sortCore0.put("missing", "0");
        JSONObject sortObj0 = new JSONObject();
        sortObj0.put("top_time", sortCore0);
        sortJson.add(sortObj0);

        JSONObject sortCore1 = new JSONObject();
        sortCore1.put("order", "asc");
        sortCore1.put("missing", MAX_DEFAULT);
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put("order", sortCore1);
        sortJson.add(sortObj1);

        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "desc");
        sortCore2.put("missing", "0");
        JSONObject sortObj2 = new JSONObject();
        sortObj2.put("is_ugc_content", sortCore2);
        sortJson.add(sortObj2);

        JSONObject sortCore3 = new JSONObject();
        sortCore3.put("order", "desc");
        sortCore3.put("missing", "0");
        JSONObject sortObj3 = new JSONObject();
        sortObj3.put(SORT_FIELD, sortCore3);
        sortJson.add(sortObj3);

        JSONObject sortCore4 = new JSONObject();
        sortCore4.put("order", "desc");
        sortCore4.put("missing", "0");
        JSONObject sortObj4 = new JSONObject();
        sortObj4.put("_id", sortCore4);
        sortJson.add(sortObj4);
    }

    @Override
    public boolean skip(OtherDataCollection baseDc) {
        if (MXStringUtils.isEmpty(baseDc.req.getResourceId())) {
            return true;
        }

        if (MXStringUtils.isEmpty(baseDc.req.getResourceType())) {
            return true;
        }

        //过滤爬虫账号
        if (baseDc.req.getResourceId().startsWith(DefineTool.CrawlerAccountEnum.CRAWLER_ACCOUNT.getPrefix())) {
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
            log.debug(String.format("VideosOfThePublisherNewRecall search url : %s", request));
            log.debug(String.format("VideosOfThePublisherNewRecall search url : %s", elasticSearchRequest));
        }

        dc.addToESRequestList(
                elasticSearchRequest,
                request,
                this.getName(), "",
                DefineTool.EsType.VIDEO_NEW.getTypeName());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(nextToken, "|");
        if (5 > tokens.length) {
            return null;
        }

        long token0 = Long.parseLong(tokens[0]);
        int token1 = Integer.parseInt(tokens[1]);
        int token2 = Integer.parseInt(tokens[2]);
        long token3 = Long.parseLong(tokens[3]);
        String token4 = tokens[4];

        result.add(token0);
        result.add(token1);
        result.add(token2);
        result.add(token3);
        result.add(token4);

        return result;
    }

    public static void main(String[] args) {
        OtherDataCollection dc = new OtherDataCollection();
        dc.req = new Request();
        dc.req.resourceId = "111";
        dc.req.resourceType = "publisher";
        new VideosOfThePublisherNewRecall().recall(dc);
    }
}