package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 音乐 track/playlist 召回基类
 */
public abstract class BaseMusicItemsRecall extends BaseRecall<OtherDataCollection> {

    @Override
    public void recall(OtherDataCollection dc) {
        // 构建 ES 请求
        String endpointSearch = DefineTool.ES.endpointSearch(getHostCategory());
        JSONObject query = constructQuery(dc);
        String request = constructContent(query, 0, getSize(), null, null).toString();

        // 拿到宿主详情
        List<JSONObject> result = MXDataSource.ES().sendSyncSearchPure(endpointSearch, request);
        if (MXJudgeUtils.isEmpty(result)) {
            return;
        }

        // 从宿主中解析内容列表
        List<BaseDocument> documents = loadItems(result.get(0));
        if (MXJudgeUtils.isEmpty(documents)) {
            return;
        }

        dc.addRecallResult(documents);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    /**
     * 加载内容
     *
     * @param host 宿主，可能是音乐卡/playlist 卡/playlist
     * @return tracks/playlists
     */
    private List<BaseDocument> loadItems(JSONObject host) {
        // 从给定字段中解析出 ID 列表
        List<String> itemIdList = BaseDocument.getStrListValue(host, getItemIdsField());
        if (MXJudgeUtils.isEmpty(itemIdList)) {
            return null;
        }

        // 生成文档实例并填充 ID
        List<BaseDocument> itemList = new ArrayList<>();
        BaseDocument itemIt;
        for (String itemIdIt : itemIdList) {
            itemIt = newItemDocument();
            itemIt.id = itemIdIt;
            itemList.add(itemIt);
        }

        return itemList;
    }

    private JSONObject getMatch(String key, String value) {
        String ret = String.format("{'match':{'%s': '%s'}}", key, value);
        return JSON.parseObject(ret);
    }

    @Override
    public JSONObject constructQuery(OtherDataCollection baseDc) {
        JSONObject query = JSON.parseObject("{'bool':{'must':[]}}");
        JSONArray musts = query.getJSONObject("bool").getJSONArray("must");
        musts.add(getMatch(BaseMagicValueEnum._id, baseDc.req.resourceId));
        return query;
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(RecallConfig.KEY_SIZE, Integer.class);
    }

    /**
     * 宿主类型
     */
    abstract DefineTool.CategoryEnum getHostCategory();

    /**
     * 放置内容元素 ID 列表的字段值
     */
    abstract String getItemIdsField();

    /**
     * 创建内容元素文档实例
     */
    abstract BaseDocument newItemDocument();
}
