package mx.j2.recommend.ruler.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

public class GetPoolIndexForDebugRulerForTrending extends BaseRuler<BaseDataCollection>{
    @Override
    public boolean skip(BaseDataCollection data) {
        return !data.isDebugModeOpen;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        Set<String> indexList = new HashSet<>();
        MXDataSource.pools().all().forEach((k, v) -> v.forEach((kk, vv) -> indexList.add(vv.poolIndex)));
        List<String> resultIdList = new ArrayList<>();
        dc.data.result.resultList.forEach(item -> resultIdList.add(item.shortVideo.id));
        ElasticSearchDataSource source = MXDataSource.ES();
        List<JSONObject> esResults = source.syncLoadDetailBySearch(resultIdList.size() * 3, resultIdList, "");
        Map<String, String> idToIndexMap = new HashMap<>();
        esResults.forEach(item -> {
            if (indexList.contains(item.getString("_index"))) {
                idToIndexMap.put(item.getString("_id"), item.getString("_index"));
            }
        });
        for (int i = 0; i < dc.data.result.resultList.size(); i++) {
            Result result = dc.data.result.resultList.get(i);
            if (idToIndexMap.containsKey(result.shortVideo.id)) {
                if (MXStringUtils.isEmpty(result.debugInfo)) {
                    result.debugInfo = "\npoolIndex: " + idToIndexMap.get(result.shortVideo.id);
                } else {
                    result.debugInfo = result.debugInfo + ",\npoolIndex: " + idToIndexMap.get(result.shortVideo.id);
                }
                dc.data.result.resultList.set(i, result);
            }
        }
    }
}
