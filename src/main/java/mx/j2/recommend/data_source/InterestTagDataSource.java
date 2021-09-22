package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InterestTagDataSource extends BaseDataSource {
    private static final String REQUEST = "/taka_flowpool_lv3_tag_v1/_search?pretty=false";
    private final Cache<String, Set<BaseDocument>> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .buildCache();

    public InterestTagDataSource() {
        init();
    }

    private void init() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(this::setTagPoolCache, 0, 5, TimeUnit.MINUTES);
    }

    public Set<BaseDocument> getInterestTagDocSet(String interestTag) {
        return recallLocalCache.get(interestTag);
    }

    private void setTagPoolCache() {
        Set<JSONObject> allEsResultSet = getAllEsResultSet();
        if (MXJudgeUtils.isEmpty(allEsResultSet)) {
            return;
        }
        Map<String, BaseDocument> idDocumentMap = getIdDocumentMap(allEsResultSet);
        if (MXJudgeUtils.isEmpty(idDocumentMap)) {
            return;
        }
        Map<String, Set<BaseDocument>> tagDocumentMap = getTagDocumentMap(allEsResultSet, idDocumentMap);
        for (String category : tagDocumentMap.keySet()) {
            recallLocalCache.put(category, tagDocumentMap.get(category));
        }
    }

    private String constructContent(String videoId, double heatScore) {
        if (videoId == null) {
            return "{\"size\":500,\"query\":{\"exists\":{\"field\":\"categories\"}},\"_source\":[\"categories\",\"heat_4\"],\"sort\":[{\"heat_4\":{\"order\":\"desc\",\"missing\":0}},\"_uid\"]}";
        }
        return "{\"size\":500,\"query\":{\"exists\":{\"field\":\"categories\"}},\"_source\":[\"categories\",\"heat_4\"],\"sort\":[{\"heat_4\":{\"order\":\"desc\",\"missing\":0}},\"_uid\"],\"search_after\":[" + heatScore + "," + "\"video#" +
                videoId + "\"]}";
    }

    private Set<JSONObject> getAllEsResultSet() {
        ElasticSearchDataSource dataSource = MXDataSource.ES();
        Set<JSONObject> allEsResultSet = new HashSet<>();
        List<JSONObject> esResultList;
        int time = 1;
        String lastVideoId = null;
        double lastHeatScore = 0D;
        while (true) {
            if (time > 100) {
                break;
            }
            String content = constructContent(lastVideoId, lastHeatScore);
            esResultList = dataSource.sendSyncSearchPure(REQUEST, content);
            if (MXJudgeUtils.isEmpty(esResultList)) {
                break;
            }
            allEsResultSet.addAll(esResultList);
            time++;
            lastVideoId = esResultList.get(esResultList.size() - 1).getString("_id");
            lastHeatScore = esResultList.get(esResultList.size() - 1).getDoubleValue("heat_4");
        }
        return allEsResultSet;
    }

    private Map<String, BaseDocument> getIdDocumentMap(Set<JSONObject> resultSet) {
        Set<String> idSet = new HashSet<>();
        resultSet.forEach(item -> idSet.add(item.getString("_id")));
        Map<String, BaseDocument> idDocumentMap = new HashMap<>();
        MXDataSource.details().get(idSet, "").forEach(item -> idDocumentMap.put(item.id, item));
        return idDocumentMap;
    }

    private Map<String, Set<BaseDocument>> getTagDocumentMap(Set<JSONObject> resultSet, Map<String, BaseDocument> idDocumentMap) {
        Map<String, Set<BaseDocument>> tagDocumentMap = new HashMap<>();
        for (JSONObject esJson : resultSet) {
            if (esJson.containsKey("categories") && esJson.containsKey("_id")) {
                for (String category : esJson.getJSONArray("categories").toJavaList(String.class)) {
                    if (MXJudgeUtils.isNotEmpty(category)) {
                        BaseDocument doc = idDocumentMap.get(esJson.getString("_id"));
                        if (doc != null) {
                            Set<BaseDocument> docSet = tagDocumentMap.getOrDefault(category, new HashSet<>());
                            docSet.add(doc);
                            tagDocumentMap.put(category, docSet);
                        }
                    }
                }
            }
        }
        return tagDocumentMap;
    }

}
