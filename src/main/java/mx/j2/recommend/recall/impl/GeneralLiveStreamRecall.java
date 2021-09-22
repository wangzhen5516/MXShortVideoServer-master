package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.LIVE_STREAM;

/**
 * @author qiqi
 * @date 2021-03-23 15:22
 */
public class GeneralLiveStreamRecall extends BaseRecall<BaseDataCollection> {

    Logger logger = LogManager.getLogger(GeneralLiveStreamRecall.class);
    private static final String SORT_FIELD = "start_time";
    private static final String ES_FORMAT = "/%s/Live/_search?pretty=false";
    private final JSONArray sortJson;
    private static final int RECALL_SIZE = 200;


    public GeneralLiveStreamRecall() {
        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject sort = new JSONObject();
        sort.put("order", "desc");
        sortCore.put(SORT_FIELD, sort);
        sortJson.add(sortCore);
    }


    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {

        String esRequest = String.format(ES_FORMAT, Conf.getCmsIndex());
        JSONObject query = constructQuery();
        String esContent = getContent(query, sortJson).toString();
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        List<JSONObject> res = elasticSearchDataSource.sendSyncSearchforLive(esRequest, esContent);
        List<LiveDocument> generalDocs = new ArrayList<>();
        List<LiveDocument> lockList = new ArrayList<>();
        for (int i = 0; i < res.size(); ++i) {
            JSONObject obj = res.get(i);
            LiveDocument doc = new LiveDocument().loadJsonObj(obj, LIVE_STREAM, this.getName(), dc);
            if (doc != null) {
                if (MXStringUtils.isNotBlank(doc.getLiveOrder())) {
                    lockList.add(doc);
                }
                generalDocs.add(doc);
            }
        }

        /*当前接口只需要普通直播间的置顶数据*/
        if ("mx_live_feed_version_1_0".equals(dc.req.interfaceName)) {
            dc.liveDocumentList.addAll(lockList);
        } else {
            dc.liveDocumentList.addAll(generalDocs);
        }
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }


    private JSONObject constructQuery() {
        JSONObject bool = new JSONObject();
        JSONObject boolObj = new JSONObject();
        JSONArray mustArr = new JSONArray();
        JSONArray mustNotArr = new JSONArray();
        JSONObject match1 = new JSONObject();
        JSONObject match2 = new JSONObject();
        JSONObject matchObj = new JSONObject();
        JSONObject matchNot = new JSONObject();
        match1.put("status", "1");
        match2.put("whitelist", "1");
        matchObj.put("match", match1);
        matchNot.put("match", match2);

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
