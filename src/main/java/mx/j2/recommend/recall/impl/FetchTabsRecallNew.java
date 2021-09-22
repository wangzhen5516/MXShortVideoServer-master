package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.CardListItemDocument;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Comparator;
import java.util.List;


public class FetchTabsRecallNew extends BaseRecall<BaseDataCollection> {
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final JSONArray SORT = JSON.parseArray("[{'order':{'order':'desc'}}]");
    //设置默认30
    private static final int DEFAULT_SIZE = 30;
    private static final int DEFAULT_MAX = 100;

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        //用单例！
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        String elsticrequest = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.CARDLISTITEM.getIndexAndType());

        JSONObject query = constructESQuery(dc);

//设置上限100
        int num = dc.req.getNum();
        if (num < 0 || num > DEFAULT_MAX) {
            num = DEFAULT_SIZE;
        }
        JSONArray nextToken = null;
        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken)) {
            nextToken = parseNextToken(dc.req.nextToken);
        }


        String request = constructContentWithNextToken(query, num, null, SORT, nextToken).toString();

//得到结果
        List<JSONObject> result = elasticSearchDataSource.sendSyncSearchforCard(elsticrequest, request);

        if (MXJudgeUtils.isEmpty(result)) {
            return;
        }
        for (JSONObject obj : result) {
            CardListItemDocument doc = new CardListItemDocument().loadJsonObj(obj, DefineTool.CategoryEnum.CARDLISTITEM, this.getName());
            if (doc != null) {
                dc.mergedList.add(doc);
            }
        }
//order逆序
        dc.mergedList.sort(Comparator.comparing((doc) -> (-((CardListItemDocument) doc).getOrder())));
        dc.syncSearchResultSizeMap.put(this.getName(), dc.mergedList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    public JSONObject constructESQuery(BaseDataCollection dc) {
        JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");

        if (MXJudgeUtils.isEmpty(dc.req.tabId)) {//TODO 兼容逻辑，后期可以改为报警
            dc.req.tabId = DefineTool.TabInfoEnum.DISCOVERY.getId();
        }
        String cmsTabId = DefineTool.TabInfoEnum.findTabInfoEnumById(dc.req.tabId, DefineTool.TabInfoEnum.DEFAULT).getTabIdProd();
        if (!DefineTool.Env.PROD.confValue.equals(Conf.getEnv())) {
            cmsTabId = DefineTool.TabInfoEnum.findTabInfoEnumById(dc.req.tabId, DefineTool.TabInfoEnum.DEFAULT).getTabIdDev();
        }
        if (MXJudgeUtils.isEmpty(cmsTabId)) {
            System.out.println("tabId is error===>" + dc.req);
        }
        musts.add(getMatch("tab_id", cmsTabId));
        musts.add(getMatch("status", "1"));
        musts.add(getMatch("platform_type", dc.req.getPlatformId()));
        return query;
    }

    public JSONObject getMatch(String key, String value) {
        String ret = String.format("{'match':{'%s': '%s'}}", key, value);
        return JSON.parseObject(ret);
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        result.add(nextToken);
        return result;
    }


}
