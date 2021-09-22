package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * 内部召回，将publisher的视频按照特定排序方法召回
 */
public class InternalSortedVideoListRecall extends InternalBaseRecall {
    private static Logger logger = LogManager.getLogger(InternalVideoDetailBaseRecall.class);

    private final static String requestUrlFormat = "/%s/_search?pretty=false";
    private static final String INDEX_URL = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
    private static final String RESOURCE_TYPE = "publisher_id";
    private static final String LOCAL_CACHE_KEY = "intenal_sorted_publisher_%s";
    private static final int RECALL_SIZE = 50;
    private JSONArray sortJson;

    public InternalSortedVideoListRecall() {
        init();
    }

    public void init() {
        sortJson = new JSONArray();

        JSONObject sortCore0 = new JSONObject();
        sortCore0.put("order", "desc");
        sortCore0.put("missing", "0");
        JSONObject sortObj0 = new JSONObject();
        sortObj0.put("share_rate_30d", sortCore0);
        sortJson.add(sortObj0);

        JSONObject sortCore1 = new JSONObject();
        sortCore1.put("order", "desc");
        sortCore1.put("missing", "0");
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put("heat_score2", sortCore1);
        sortJson.add(sortObj1);
    }

    @Override
    public void recall(InternalDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.internalReq.resourceIdList)) {
            return;
        }

        String cacheKey = String.format(LOCAL_CACHE_KEY, dc.internalReq.resourceIdList.get(0));
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> documentList = localCacheDataSource.getInternalSortedVideoListCache(cacheKey);
        if (MXJudgeUtils.isNotEmpty(documentList)) {
            dc.mergedList.addAll(documentList);
            return;
        }

        constructRequestURL(dc);
    }

    private void constructRequestURL(InternalDataCollection dc) {
        String elasticSearchRequest = String.format(requestUrlFormat, INDEX_URL);
        assembleRequest(dc, elasticSearchRequest, dc.internalReq.resourceIdList.get(0));
        dc.searchEngineRecallerSet.add(this.getName());
    }

    private void assembleRequest(InternalDataCollection dc, String elasticSearchRequest, String publisherId) {
        if (MXJudgeUtils.isBlank(publisherId)) {
            return;
        }
        JSONObject query = constructQuery(publisherId);
        String request = query.toJSONString();

        dc.addToESRequestList(
                elasticSearchRequest,
                request,
                this.getName(), "",
                DefineTool.EsType.VIDEO.getTypeName());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private JSONObject constructQuery(String value) {
        JSONObject field = new JSONObject();
        JSONObject match = new JSONObject();
        field.put(RESOURCE_TYPE, value);
        match.put("match", field);

        JSONObject content = new JSONObject();
        content.put("query", match);
        content.put("size", RECALL_SIZE);
        content.put("sort", sortJson);

        return content;
    }

    public static void main(String[] args) {
        String INDEX_FORMAT = "takatak_simple_trigger_v%s";
        int hash = "15112368644019668".hashCode() & Integer.MAX_VALUE;
        String indexUrl = String.format(INDEX_FORMAT, hash % 16);
        System.out.println(indexUrl);
    }
}
