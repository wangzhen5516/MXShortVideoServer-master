package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.LIVE_STREAM;

public class LiveStreamFollowRecall extends BaseRecall<BaseDataCollection> {
    Logger logger = LogManager.getLogger(LiveStreamFollowRecall.class);
    private static final String SORT_FIELD = "start_time";
    private static final String ES_FORMAT = "/%s/Live/_search?pretty=false";
    private final JSONArray sortJson;
    private static final int RECALL_SIZE = 200;


    public LiveStreamFollowRecall() {
        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject sort = new JSONObject();
        sort.put("order", "desc");
        sortCore.put(SORT_FIELD, sort);
        sortJson.add(sortCore);
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return !MXJudgeUtils.isLogin(data);
    }

    @Override
    public void recall(BaseDataCollection dc) {

        String userId = null;
        if (dc.req != null && dc.req.getUserInfo() != null && MXStringUtils.isNotBlank(dc.req.getUserInfo().getUserId())) {
            userId = dc.req.getUserInfo().getUserId();
        }

        String localKey = String.format("%s_%s", this.getName(), dc.req.interfaceName);
        LocalCacheDataSource dataSource = MXDataSource.cache();
        List<LiveDocument> generalDocs = dataSource.getLiveCache(localKey);

        if (MXCollectionUtils.isEmpty(generalDocs)) {
            generalDocs = new ArrayList<>();
            String esRequest = String.format(ES_FORMAT, Conf.getCmsIndex());
            JSONObject query = constructQuery();
            String esContent = getContent(query, sortJson).toString();
            ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
            List<JSONObject> res = elasticSearchDataSource.sendSyncSearchforLive(esRequest, esContent);
            for (int i = 0; i < res.size(); ++i) {
                JSONObject obj = res.get(i);
                LiveDocument doc = new LiveDocument().loadJsonObj(obj, LIVE_STREAM, this.getName(), dc);
                if (doc != null) {
                    generalDocs.add(doc);
                }
            }
            dataSource.setLiveCache(localKey,generalDocs);
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < generalDocs.size(); ++i) {
            LiveDocument doc = generalDocs.get(i);
            if (MXStringUtils.isNotBlank(doc.publisher_id)) {
                if (i == generalDocs.size() - 1) {
                    builder.append(doc.publisher_id);
                } else {
                    builder.append(doc.publisher_id);
                    builder.append(",");
                }
            }
        }

        List<String> followers = isFollowUtil.getFollowedIds(userId, builder.toString());
        List<LiveDocument> followerDocs = new ArrayList<>();

        if (MXCollectionUtils.isEmpty(followers)) {
            return;
        }

        for (LiveDocument doc : generalDocs) {
            if (doc != null && MXStringUtils.isNotBlank(doc.publisher_id) && followers.contains(doc.publisher_id)) {
                if (doc.getLiveWhiteList() == 1) {
                    doc.setLiveScore(4);
                } else {
                    doc.setLiveScore(3);
                }
                doc.setFollow(true);
                followerDocs.add(doc);
            }
        }

        if (MXCollectionUtils.isEmpty(followerDocs)) {
            return;
        }

        dc.liveDocumentList.addAll(followerDocs);
        dc.data.response.setResultNum(followerDocs.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }


    private JSONObject constructQuery() {
        JSONObject bool = new JSONObject();
        JSONObject boolObj = new JSONObject();
        JSONArray mustArr = new JSONArray();
        JSONArray mustNotArr = new JSONArray();
        JSONObject match1 = new JSONObject();
        JSONObject matchObj = new JSONObject();
        JSONObject matchNot = new JSONObject();
        match1.put("status", "1");
        matchObj.put("match", match1);

        mustArr.add(matchObj);
        mustNotArr.add(matchNot);
        boolObj.put("must", mustArr);
        boolObj.put("must_not", mustNotArr);
        bool.put("bool", boolObj);
        return bool;
    }

    private JSONObject getContent(JSONObject query, JSONArray sort) {
        JSONObject content = new JSONObject();
        if (MXCollectionUtils.isNotEmpty(query)) {
            content.put("query", query);
        }
        if (MXCollectionUtils.isNotEmpty(sort)) {
            content.put("sort", sort);
        }
        content.put("size", RECALL_SIZE);
        return content;
    }
}
