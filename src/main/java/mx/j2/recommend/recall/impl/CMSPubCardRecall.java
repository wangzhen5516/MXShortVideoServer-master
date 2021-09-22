package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.PublisherInfo;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-05-21
 */

public class CMSPubCardRecall extends BaseRecall<BaseDataCollection> {
    private static final Logger logger = LogManager.getLogger(CMSPubCardRecall.class);

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final String CACHE_KEY_FORMAT = "CMS_PUBID_%s";

    @Override
    public void recall(BaseDataCollection dc) {
        List<String> publisherIds;

        //取缓存
        String cardId = dc.req.getResourceId();
        String cacheKey = String.format(CACHE_KEY_FORMAT, cardId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        publisherIds = localCacheDataSource.getcmsPubCardPubIdsCache(cacheKey);

        if (MXJudgeUtils.isEmpty(publisherIds)) {
            ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
            String elsticrequest = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.CARD.getIndexAndType());
            String request = constructESQuery(dc).toString();
            List<JSONObject> result = elasticSearchDataSource.sendSyncSearchforCard(elsticrequest, request);

            if (MXJudgeUtils.isEmpty(result)) {
                return;
            }
            for (JSONObject obj : result) {
                JSONArray pubIDS = obj.getJSONArray("publisher_ids");
                if (MXJudgeUtils.isEmpty(pubIDS)) {
                    return;
                }
                publisherIds = pubIDS.toJavaList(String.class);
                //存入缓存
                localCacheDataSource.setCmsPubCardPubIdsCache(cacheKey, publisherIds);
            }
        }

        if (MXJudgeUtils.isEmpty(publisherIds)) {
            return;
        }

        String nextToken = dc.req.nextToken;
        int num = dc.req.num;
        if (nextToken != null) {
            for (int i = 0; i < publisherIds.size(); i++) {
                if (nextToken.equals(publisherIds.get(i))) {
                    if (i + 1 < publisherIds.size()) {
                        publisherIds = new ArrayList<>(publisherIds.subList(i + 1, publisherIds.size()));
                    }else {
                        publisherIds = null;
                    }
                    break;
                }
            }
        }

        if (MXJudgeUtils.isEmpty(publisherIds)) {
            return;
        }
        List<PublisherInfo> publisherInfos = constructPublisherInfo(publisherIds);

        dc.cmsPubCardPubIds.addAll(publisherInfos);
    }

    public JSONObject constructESQuery(BaseDataCollection dc) {
        JSONObject content = new JSONObject();

        String cardId = dc.req.getResourceId();
        JSONObject match = new JSONObject();
        JSONObject _id = new JSONObject();
        _id.put("_id", cardId);
        match.put("match", _id);
        content.put("query", match);

        return content;
    }

    private List<PublisherInfo> constructPublisherInfo(List<String> publisherIds) {
        List<PublisherInfo> publisherInfos = new ArrayList<>();
        for (String publisherId : publisherIds) {
            PublisherInfo publisherInfo = new PublisherInfo();
            JSONObject object = new JSONObject();
            object.put("name", "");
            publisherInfo.setId(publisherId);
            publisherInfo.setReason(object.toJSONString());
            publisherInfos.add(publisherInfo);
        }
        return publisherInfos;
    }
}
