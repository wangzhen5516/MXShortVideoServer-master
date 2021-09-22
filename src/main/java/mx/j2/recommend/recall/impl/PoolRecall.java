package mx.j2.recommend.recall.impl;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;

import java.util.*;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:41 下午 2020/8/19
 */
public class PoolRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall {

    private static final Logger log = LogManager.getLogger(PoolRecall.class);

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final int RECALL_WEIGHT = 1200;
    private static final int RECALL_SIZE = 200;
    private static final int RECALL_FROM = 0;
    private static final JSONArray DEFAULT_SORT_JSON;

    private static final String SORT_STRING_PATTERN = "[{ \"_uid\": { \"missing\": \"0\", \"order\": \"desc\" } }]";

    private final static int RANDOM_FACTOR = new Random().nextInt(10);

    static {
        DEFAULT_SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("script", "Math.random()");
        script.put("type", "number");
        script.put("order", "asc");
        sortCore.put("_script", script);
        DEFAULT_SORT_JSON.add(sortCore);
    }

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        Map<String, Map<String, PoolConf>> map = MXDataSource.pools().all();
        for (Map.Entry<String, Map<String, PoolConf>> entry : map.entrySet()) {
            Map<String, PoolConf> value = entry.getValue();
            PoolConf pc = value.getOrDefault(baseDc.recommendFlow.name, value.get("base"));
            if (pc == null) {// should not run
                continue;
            }
            baseDc.poolConfMap.put(pc.poolIndex, pc);

            if (pc.userLevel != null && pc.userLevel.size() > 0) {// userLevel为空时，不走过滤
                // 新用户，必须是pool.json中配置着new才出这个级别池子的数据
                if (UserProfileDataSource.isPureNewUser(baseDc) && !pc.userLevel.contains(DefineTool.PoolConfUserLevel.NEW)) {
                    continue;
                }
                // 老用户，必须配置old才出这个级别的数据
                if (!UserProfileDataSource.isPureNewUser(baseDc) && !pc.userLevel.contains(DefineTool.PoolConfUserLevel.OLD)) {
                    continue;
                }
            }

            List<BaseDocument> mergedList = MXDataSource.flowPool().getFollowContent(pc.poolIndex + "_" + pc.sortField);

            if (MXJudgeUtils.isNotEmpty(mergedList)) {
                //设置成tophot历史过滤
                if (pc.isTophotHistory) {
                    String curPoolLevel = getPoolLevel(pc.poolLevel);
                    for (BaseDocument doc : mergedList) {
                        if (ObjectUtil.isNull(doc)) {
                            continue;
                        }
                        // TODO add filter for tophot
                        // TODO need modify the super big POOL's "detail loader"
                        if (doc.statisticsDocument.exist(FEATURE30D)
                                && doc.statisticsDocument.get(FEATURE30D).getViewAll() > 5000000) {
                            doc.setTopHotHistory(true);
                        }
                        doc.setPriority(pc.priority);
                        doc.poolLevel = curPoolLevel;
                        doc.setPoolIndex(pc.poolIndex);
                        doc.setPoolPriority(pc.priority);

                        // 调试信息
                        MXEntityDebugInfo debugInfo = baseDc.debug.getDebugInfoByEntityId(doc.id);
                        debugInfo.recall.name = getName();
                    }
                } else {
                    addPriority(baseDc, mergedList, pc.priority, pc.poolLevel, pc.poolIndex);
                }
                baseDc.poolToDocumentListMap.put(pc.poolIndex, mergedList);
                baseDc.syncSearchResultSizeMap.put(this.getName() + pc.poolIndex + "_" + pc.sortField, mergedList.size());
            }
        }

        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private void addPriority(BaseDataCollection baseDc,
                             List<BaseDocument> documents,
                             int level,
                             JSONArray poolLevel,
                             String poolIndex) {
        if (MXJudgeUtils.isEmpty(documents)) {
            return;
        }
        String curPoolLevel = getPoolLevel(poolLevel);
        documents.forEach(doc -> {
            doc.setPriority(level);
            doc.setPoolLevel(curPoolLevel);
            doc.setPoolPriority(level);
            doc.setPoolIndex(poolIndex);

            // 调试信息
            MXEntityDebugInfo debugInfo = baseDc.debug.getDebugInfoByEntityId(doc.id);
            debugInfo.recall.name = getName();
        });
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT;
    }

    public static void main(String[] args) {
        System.out.println(new PoolRecall().constructContent(null, RECALL_FROM, RECALL_SIZE, null, DEFAULT_SORT_JSON).toString());
    }

    @Override
    public String getRecallName() {
        return this.getName();
    }

    @Override
    public float getRecallDocumentWeight() {
        return 0;
    }

    @Override
    public Map<String, BaseDataCollection.ESRequest> getESRequestMap() {
        Map<String, BaseDataCollection.ESRequest> esRequestMap = new HashMap<>();
        Map<String, Map<String, PoolConf>> map = MXDataSource.pools().all();
        if (MXJudgeUtils.isEmpty(map)) {
            return esRequestMap;
        }

        //TODO only search uni's poolIndex
        for (Map.Entry<String, Map<String, PoolConf>> entry : map.entrySet()) {
            Set<String> poolIndexSet = new HashSet<>();
            for (PoolConf pc : entry.getValue().values()) {
                if (poolIndexSet.contains(pc.poolIndex + "_" + pc.sortField)) {
                    continue;
                }
                poolIndexSet.add(pc.poolIndex + "_" + pc.sortField);
                BaseDataCollection.ESRequest esRequest = constructRequest(pc);
                esRequestMap.put(pc.poolIndex + "_" + pc.sortField, esRequest);
            }
        }
        return esRequestMap;
    }

    @Override
    public Map<String, SearchSourceBuilder> getSearchSourceBuilderMap() {
        Map<String, SearchSourceBuilder> builderMap = new HashMap<>();
        Map<String, Map<String, PoolConf>> confMap = MXDataSource.pools().all();
        if (MXJudgeUtils.isEmpty(confMap)) {
            return builderMap;
        }

        for (Map.Entry<String, Map<String, PoolConf>> entry : confMap.entrySet()) {
            Set<String> poolIndexSet = new HashSet<>();
            for (PoolConf pc : entry.getValue().values()) {
                if (poolIndexSet.contains(pc.poolIndex + "_" + pc.sortField)) {
                    continue;
                }
                poolIndexSet.add(pc.poolIndex + "_" + pc.sortField);
                SearchSourceBuilder builder = buildBuilder(pc);
                builderMap.put(pc.poolIndex + "_" + pc.sortField, builder);
            }
        }
        return builderMap;
    }

    private SearchSourceBuilder buildBuilder(PoolConf pc) {
        if (MXJudgeUtils.isNotEmpty(pc.sortField)) {

        }
        Script script = new Script("Math.random()");
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(script, ScriptSortBuilder.ScriptSortType.NUMBER);
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.size(pc.poolRecallSize);

        return searchSourceBuilder;
    }

    @Override
    public void doSomethingAfterLoad() {
    }

    private BaseDataCollection.ESRequest constructRequest(PoolConf pc) {

        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, pc.poolIndex);
        String content = "";
        if (MXStringUtils.isNotEmpty(pc.sortField)) {
            try {
                JSONArray sortArray = JSONArray.parseArray(SORT_STRING_PATTERN);
                sortArray.addAll(0, pc.sortFieldNew);
                JSONObject contentTemp = constructContent(null, RECALL_FROM, pc.poolRecallSize, null, sortArray);
                contentTemp.put("_source", false);
                content = contentTemp.toString();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (MXStringUtils.isEmpty(content)) {
            content = constructContent(null, RECALL_FROM, pc.poolRecallSize, null, DEFAULT_SORT_JSON).toString();
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("PoolRecall search url : %s", content));
            log.debug(String.format("PoolRecall search url : %s", elasticSearchRequest));
        }

        return new BaseDataCollection.ESRequest(elasticSearchRequest, content, this.getRecallName(), "", "pool", pc.poolIndex);
    }

    @Override
    public int scheduledPeriodSeconds() {
        return DefineTool.ScheduledPeriodSeconds.TenSeconds.getSeconds();
    }

    @Override
    public int getRandomFactor() {
        return RANDOM_FACTOR;
    }

    /**
     * 获取池子级别
     *
     * @param poolLevel
     * @return
     */
    private String getPoolLevel(JSONArray poolLevel) {
        StringBuilder builder = new StringBuilder();
        try {
            for (int i = 0; i < poolLevel.size(); ++i) {
                String poolLevelString = poolLevel.getString(i);
                if (i == poolLevel.size() - 1) {
                    builder.append(poolLevelString);
                } else {
                    builder.append(poolLevelString);
                    builder.append("_");
                }
            }
            return builder.toString();
        } catch (Exception e) {
            log.error("get poolLevel error", e);
        }
        return null;
    }
}
