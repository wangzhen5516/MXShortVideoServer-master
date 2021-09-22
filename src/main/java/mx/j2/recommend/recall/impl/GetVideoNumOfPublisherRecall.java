package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_source.AWSSqsDataSource;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.PublisherCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 获得publisher的视频数量
 *
 * @author xiang.zhou
 */
public class GetVideoNumOfPublisherRecall extends BaseRecall<OtherDataCollection> {
    private static Logger log = LogManager.getLogger(GetVideoNumOfPublisherRecall.class);

    private final static int CACHE_TIME_SECONDS = 10;
    private static final int RECALL_FROM = 0;
    private final static String INDEX_FORMAT = "/takatak_simple_trigger_v%s/_search?pretty=false";
    private static final String PUB_TYPE = "public";

    @Override
    public boolean skip(OtherDataCollection dc) {
        String publisherId = dc.req.getResourceId();
        if (MXStringUtils.isBlank(publisherId)) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(OtherDataCollection dc) {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        String publisherId = dc.req.getResourceId();

        String cacheKey = construcCacheKey(publisherId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        /*local*/
        Integer localNum = localCacheDataSource.getNumOfVideoPublisher(cacheKey);
        if (localNum != null) {
            dc.data.response.setResultNum(localNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), localNum);
            return;
        }
        /*redis*/
        Integer redisNum = elasticCacheSource.getNumOfPubCache(cacheKey);
        if (null != redisNum) {
            dc.data.response.setResultNum(redisNum);
            localCacheDataSource.setNumOfVideoPublisher(cacheKey, redisNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), redisNum);
            return;
        }
        /*从ca里获取数据*/
        PublisherCassandraDataSource publisherCaDataSource = MXDataSource.publisherCA();
        Integer caNum = publisherCaDataSource.getVideoNumOfPub(publisherId, PUB_TYPE);
        if (caNum != null) {
            dc.data.response.setResultNum(caNum);
            localCacheDataSource.setNumOfVideoPublisher(cacheKey, caNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), caNum);
            elasticCacheSource.setNumOfPubCache(cacheKey, String.valueOf(caNum));
            return;
        }
        /*ca里没数据则走es拉*/
        int hash = publisherId.hashCode() & Integer.MAX_VALUE;
        String elasticSearchRequest = String.format(INDEX_FORMAT, hash % 16);
        JSONObject query = constructQuery(dc);
        String request = constructContent(query, RECALL_FROM, 0, null, null).toString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("UgcRecall search url : %s", request));
            log.debug(String.format("UgcRecall search url : %s", elasticSearchRequest));
        }

        int num = MXDataSource.videoESV7()
                .sendSyncOnlyReturnTotal(elasticSearchRequest, request);
        try {
            /*回写ca*/
            AWSSqsDataSource awsSqsDataSource = MXDataSource.SQS();
            awsSqsDataSource.sendMessage(publisherId);
        } catch (Exception e) {
            log.error(String.format("public video num send message error,id is:%s", publisherId), e);
        }
        /*存redis/cache*/
        localCacheDataSource.setNumOfVideoPublisher(cacheKey, num);
        elasticCacheSource.setNumOfPubCache(cacheKey, String.valueOf(num));
        dc.data.response.setResultNum(num);
        dc.syncSearchResultSizeMap.put(this.getName(), num);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    public JSONObject getMatch(String key, String value) {
        String ret = String.format("{'match':{'%s': '%s'}}", key, value);
        return JSON.parseObject(ret);
    }

    @Override
    public JSONObject constructQuery(OtherDataCollection baseDc) {
        //JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONObject query = JSON.parseObject("{'bool':{'must':[],'must_not':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");
        JSONArray mustsNot = query.getJSONObject("bool").getJSONArray("must_not");
        musts.add(getMatch("publisher_id", baseDc.req.getResourceId()));
        if ("online".equals(baseDc.req.getResourceType())) {
            musts.add(getMatch("status", "1"));
        }
        mustsNot.add(getMatch("view_privacy", "2"));
        return query;
    }

    private String construcCacheKey(String publisherId) {
        return String.format("%s_%s", publisherId, this.getName());
    }

    public static void main(String[] args) {
        OtherDataCollection baseDc = new OtherDataCollection();
        baseDc.req = new Request();
        baseDc.req.setResourceId("111");
        baseDc.req.setResourceType("online");
        JSONObject ret = new GetVideoNumOfPublisherRecall().constructQuery(baseDc);
        System.out.println(ret);
    }

}
