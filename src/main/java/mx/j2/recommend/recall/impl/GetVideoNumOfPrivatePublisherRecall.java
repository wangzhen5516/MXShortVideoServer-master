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
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 获得publisher页面private视频的数量
 *
 * @author DuoZhao
 */
public class GetVideoNumOfPrivatePublisherRecall extends BaseRecall<OtherDataCollection> {
    private static Logger log = LogManager.getLogger(GetVideoNumOfPrivatePublisherRecall.class);

    private final static String INDEX_FORMAT = "/takatak_simple_trigger_v%s/_search?pretty=false";
    private static final int RECALL_FROM = 0;
    private static final String PUB_TYPE = "private";
    private static final ElasticCacheSource elasticCacheSource = MXDataSource.redis();

    @Override
    public boolean skip(OtherDataCollection dc) {
        if (dc.req == null) {
            return true;
        }
        String publisherId = dc.req.getResourceId();
        if (MXStringUtils.isBlank(publisherId)) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(OtherDataCollection dc) {
        String publisherId = dc.req.getResourceId();

        String cacheKey = construcCacheKey(dc);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        /*local*/
        Integer localNum = localCacheDataSource.getNumOfVideoPublisher(cacheKey);
        if (localNum != null) {
            dc.data.response.setResultNum(localNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), localNum);
            return;
        }

        Integer redisNum = elasticCacheSource.getNumOfPubCache(cacheKey);
        if (null != redisNum) {
            dc.data.response.setResultNum(redisNum);
            localCacheDataSource.setNumOfVideoPublisher(cacheKey, redisNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), redisNum);
            return;
        }

        PublisherCassandraDataSource publisherCadraDataSource = MXDataSource.publisherCA();
        Integer newNum = publisherCadraDataSource.getVideoNumOfPub(publisherId, PUB_TYPE);
        if (newNum != null) {
            dc.data.response.setResultNum(newNum);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), newNum);
            elasticCacheSource.setNumOfPubCache(cacheKey, String.valueOf(newNum));
            localCacheDataSource.setNumOfVideoPublisher(cacheKey, newNum);
            return;
        }
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
        /*回写ca*/
        AWSSqsDataSource awsSqsDataSource = MXDataSource.SQS();
        try {
            awsSqsDataSource.sendMessage(publisherId);
        } catch (Exception e) {
            log.error(String.format("private video num send msg error,id is %s", publisherId), e);
        }
        elasticCacheSource.setNumOfPubCache(cacheKey, String.valueOf(num));
        localCacheDataSource.setNumOfVideoPublisher(cacheKey, num);
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
        JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");
        musts.add(getMatch("publisher_id", baseDc.req.getResourceId()));
        if ("online".equals(baseDc.req.getResourceType())) {
            musts.add(getMatch("status", "1"));
        }
        musts.add(getMatch("view_privacy", "2"));
        return query;
    }

    private String construcCacheKey(OtherDataCollection baseDc) {
        return String.format("%s_%s", baseDc.req.getResourceId(), this.getName());
    }

}
