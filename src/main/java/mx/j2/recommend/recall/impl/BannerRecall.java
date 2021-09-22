package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BannerDataCollection;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BannerDocument;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.BANNER;

/**
 * 获得publisher的视频数量
 * @author xiang.zhou
 */
public class BannerRecall extends BaseRecall<BannerDataCollection> {
    private static Logger log = LogManager.getLogger(BannerRecall.class);

    private final static int CACHE_TIME_SECONDS = 120;
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final int RECALL_FROM = 0;

    private JSONArray sortJson;

    public BannerRecall() {
        init();
    }

    @Override
    public void init() {
        sortJson = new JSONArray();
        JSONObject sortCore1 = new JSONObject();
        sortCore1.put("order", "desc");
        sortCore1.put("missing", "_last");
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put("order", sortCore1);

        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "desc");
        sortCore2.put("missing", "_last");
        JSONObject sortObj2 = new JSONObject();
        sortObj2.put("create_time", sortCore2);

        sortJson.add(sortObj1);
        sortJson.add(sortObj2);
    }

    @Override
    public boolean skip(BannerDataCollection dc) {
        return dc.req.getPlatformId() == null;
    }

    @Override
    public void recall(BannerDataCollection dc) {
        String cacheKey  = construcCacheKey(dc);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            dc.mergedList.addAll(cacheDocumentList);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, BANNER.getIndexAndType());

        JSONObject query = constructQuery(dc);
        String request = constructContent(query, RECALL_FROM, 10, null, sortJson).toString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("BannerRecall search url : %s", request));
            log.debug(String.format("BannerRecall search url : %s", elasticSearchRequest));
        }

        //TODO: add cache
        List<JSONObject> result = MXDataSource.ES().sendSyncSearchPure(elasticSearchRequest, request);
        List<BaseDocument> documents = new ArrayList<>();
        for (JSONObject obj : result) {
            BannerDocument doc = new BannerDocument().loadJsonObj(obj, BANNER, this.getName());
            if (null != doc) {
                documents.add(doc);
            }
        }
        dc.mergedList.addAll(documents);
        localCacheDataSource.setScoreWeightRecallCache(cacheKey, documents, CACHE_TIME_SECONDS);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    public JSONObject getMatch(String key, String value) {
        String ret = String.format("{'match':{'%s': '%s'}}", key, value);
        return JSON.parseObject(ret);
    }

    @Override
    public JSONObject constructQuery(BannerDataCollection baseDc) {
        JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");
        if (MXJudgeUtils.isEmpty(baseDc.req.tabId)) {//TODO 兼容逻辑，后期可以改为报警
            baseDc.req.tabId = DefineTool.TabInfoEnum.DISCOVERY.getId();
        }
        String cmsTabId = DefineTool.TabInfoEnum.findTabInfoEnumById(baseDc.req.tabId, DefineTool.TabInfoEnum.DEFAULT).getTabIdProd();
        if (!DefineTool.Env.PROD.confValue.equals(Conf.getEnv())) {
            cmsTabId = DefineTool.TabInfoEnum.findTabInfoEnumById(baseDc.req.tabId, DefineTool.TabInfoEnum.DEFAULT).getTabIdDev();
        }
        if (MXJudgeUtils.isEmpty(cmsTabId)) {
            System.out.println("tabId is error===>" + baseDc.req);
        }
        musts.add(getMatch("tab_id", cmsTabId));
        musts.add(getMatch("status", "1"));
        musts.add(getMatch("platform_type", baseDc.req.getPlatformId()));
        long nowTime = System.currentTimeMillis();
        musts.add(JSON.parseObject(String.format("{\"range\":{\"end_time\":{\"gte\":%d}}}", nowTime)));
        musts.add(JSON.parseObject(String.format("{\"range\":{\"start_time\":{\"lte\":%d}}}", nowTime)));
        return query;
    }

    private String construcCacheKey(BaseDataCollection baseDc){
        return String.format("%s_%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), baseDc.req.getTabId(), baseDc.req.getPlatformId());
    }

    public static void main(String[] args) {
        BannerDataCollection baseDc = new BannerDataCollection();
        baseDc.req = new Request();
        baseDc.req.setPlatformId("1");
        JSONObject ret = new BannerRecall().constructQuery(baseDc);
        System.out.println(ret);

        BannerRecall recaller = new BannerRecall();
        System.out.println(recaller.sortJson);
    }

}
