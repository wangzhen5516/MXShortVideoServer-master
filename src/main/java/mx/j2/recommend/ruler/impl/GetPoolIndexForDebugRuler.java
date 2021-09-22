package mx.j2.recommend.ruler.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

public class GetPoolIndexForDebugRuler extends BaseRuler<OtherDataCollection> {

    private static final int MAX_SIZE = 3;

    @Override
    public boolean skip(OtherDataCollection data) {
        return !data.isDebugModeOpen;
    }

    @Override
    public void rule(OtherDataCollection dc) {

        List<String> resultIdList = new ArrayList<>();
        dc.data.result.resultList.forEach(item -> resultIdList.add(item.shortVideo.id));
        ElasticSearchDataSource source = MXDataSource.ES();
        List<JSONObject> esResults = source.syncLoadDetailBySearch(2000, resultIdList, "");
        Map<String, List<String>> idToIndexMap = new HashMap<>();
        esResults.forEach(item -> {
            if (MXStringUtils.isNotBlank(item.getString("_id")) && MXStringUtils.isNotBlank(item.getString("_index"))) {
                String id = item.getString("_id");
                String index = item.getString("_index");
                if (idToIndexMap.containsKey(id)) {
                    List<String> list = idToIndexMap.get(id);
                    list.add(index);
                    idToIndexMap.put(id, list);
                } else {
                    List<String> newList = new ArrayList<>();
                    newList.add(index);
                    idToIndexMap.put(id, newList);
                }

            }
        });

        for (int i = 0; i < dc.data.result.resultList.size(); i++) {
            Result result = dc.data.result.resultList.get(i);
            if (idToIndexMap.containsKey(result.shortVideo.id)) {
                List<String> poolIndexList = sortPoolIndexList(idToIndexMap.get(result.shortVideo.id));
                if (MXStringUtils.isEmpty(result.debugInfo)) {
                    result.debugInfo = "\npoolIndex: " + packPoolIndex(poolIndexList);
                } else {
                    result.debugInfo = result.debugInfo + ",\npoolIndex: " + packPoolIndex(poolIndexList);
                }
                dc.data.result.resultList.set(i, result);
            }
        }
    }

    /**
     * 将list数据换行拼接
     *
     * @param poolIndex
     * @return
     */
    private String packPoolIndex(List<String> poolIndex) {
        if (MXCollectionUtils.isEmpty(poolIndex)) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (String str : poolIndex) {
            builder.append("\n").append(str).append(",");
        }
        return builder.toString();
    }

    /**
     * sort
     *
     * @param poolIndex
     * @return
     */
    private List<String> sortPoolIndexList(List<String> poolIndex) {
        if (MXCollectionUtils.isEmpty(poolIndex)) {
            return null;
        }
        /*1.按照字符串逆序2.按照字符串长度逆序3.截断*/
        Collections.sort(poolIndex);
        Collections.reverse(poolIndex);
        poolIndex.sort((s1, s2) -> s2.length() - s1.length());
        if (poolIndex.size() > MAX_SIZE) {
            poolIndex = poolIndex.subList(0, MAX_SIZE);
        }
        return poolIndex;
    }
}
