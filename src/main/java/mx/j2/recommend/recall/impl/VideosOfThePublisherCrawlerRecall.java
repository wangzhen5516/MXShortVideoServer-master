package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static mx.j2.recommend.util.BaseMagicValueEnum.SCORE_30D;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/9 下午4:59
 * @description
 */
public class VideosOfThePublisherCrawlerRecall extends SearchEngineRecall<OtherDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfThePublisherCrawlerRecall.class);
    private String requestUrlFormat = "";
    private JSONArray sortJson;
    private final static int RECALL_SIZE = DefineTool.Recall.Config.SizeEnum.VIDEOS_OF_PUBLISHER.configValue;
    private final static int RECALL_FROM = 0;

    private final static String INDEX_FORMAT = "takatak_simple_trigger_v%s";
    private final static String INTERFACE_NAME = "mx_videos_of_the_publisher_me_version_1_0";

    /**
     * 构造函数
     */
    public VideosOfThePublisherCrawlerRecall() {
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
        sortCore1.put("order", "desc");
        sortCore1.put("missing", "-0.1");
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put(SCORE_30D, sortCore1);
        sortJson.add(sortObj1);

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

        //过滤ugc账号
        if (!baseDc.req.getResourceId().startsWith(DefineTool.CrawlerAccountEnum.CRAWLER_ACCOUNT.getPrefix())) {
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
            log.debug(String.format("VideosOfThePublisherCrawlerRecall search url : %s", request));
            log.debug(String.format("VideosOfThePublisherCrawlerRecall search url : %s", elasticSearchRequest));
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
        if (3 > tokens.length) {
            return null;
        }

        String token0 = tokens[0];
        String token1 = tokens[1];
        String token2 = tokens[2];

        result.add(token0);
        result.add(token1);
        result.add(token2);

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
