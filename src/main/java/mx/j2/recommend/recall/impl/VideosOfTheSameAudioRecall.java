package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2018/12/5
 * @ Description：${description}
 */
public class VideosOfTheSameAudioRecall extends SearchEngineRecall<OtherDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfTheSameAudioRecall.class);

    private String requestUrlFormat = "";
    private String requestUrlFormatMusic = "";
    private JSONArray sortJson;
    private final static int RECALL_SIZE = 50;
    private final static String SORT_FIELD = "online_time";
    private final static DefineTool.CategoryEnum CATEGORY_ENUM = DefineTool.CategoryEnum.SHORT_VIDEO;

    private final String BIND_AUDIO_CACHE_KEY = "bind_audio_cache_key";
    private final String BIND_AUDIO_REDIS_KEY = "bind_audio_table";

    /**
     * 构造函数
     */
    public VideosOfTheSameAudioRecall() {
        init();
    }

    /**
     * 初始化
     */
    @Override
    public void init() {
        requestUrlFormat = "/%s/_search?pretty=false";
        requestUrlFormatMusic = "/%s/_search?pretty=false";

        sortJson = new JSONArray();
        JSONObject sortCore1 = new JSONObject();
        sortCore1.put("order", "desc");
        JSONObject sortObj1 = new JSONObject();
        sortObj1.put("is_original_audio", sortCore1);
        sortJson.add(sortObj1);

        JSONObject sortCore4 = new JSONObject();
        sortCore4.put("order", "desc");
        sortCore4.put("missing", "0");
        JSONObject sortObj4 = new JSONObject();
        sortObj4.put("heat_score", sortCore4);
        sortJson.add(sortObj4);

        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "desc");
        JSONObject sortObj2 = new JSONObject();
        sortObj2.put(SORT_FIELD, sortCore2);
        sortJson.add(sortObj2);

//        JSONObject sortCore3 = new JSONObject();
//        sortCore3.put("order", "desc");
//        JSONObject sortObj3 = new JSONObject();
//        sortObj3.put("_uid", sortCore3);
//        sortJson.add(sortObj3);
    }

    @Override
    public boolean skip(OtherDataCollection baseDc) {
        if (MXJudgeUtils.isEmpty(baseDc.req.getResourceId())) {
            return true;
        }

        return MXJudgeUtils.isEmpty(baseDc.req.getResourceType());
    }

    @Override
    @Trace(dispatcher = true)
    public void constructRequestURL(OtherDataCollection baseDc) {
        String indexUrl = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
        String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);
        String masterAudio = findMasterAudio(baseDc.req.getResourceId());
        List<String> bindAudioIds = new ArrayList<>();
        if (MXJudgeUtils.isNotEmpty(masterAudio)) {
            bindAudioIds = getBindAudioIds(masterAudio);
        }

        Map<String, String> audioCondition = new HashMap<>();
        audioCondition.put(baseDc.req.getResourceType() + "_id", baseDc.req.getResourceId());
        assembleRequest(baseDc, elasticSearchRequest, audioCondition, bindAudioIds);

        baseDc.searchEngineRecallerSet.add(this.getName());
    }

    /**
     * 填充dc中相应的请求内容
     */
    private void assembleRequest(OtherDataCollection dc, String elasticSearchRequest, Map<String, String> conditionMap, List<String> bindAudioIds) {
        JSONObject query;
        if (MXJudgeUtils.isNotEmpty(bindAudioIds) && bindAudioIds.size() >= 2) {
            query = constructQueryByConditionFilterPrivateVideoTerms(dc.req.getResourceType() + "_id", bindAudioIds);
        } else {
            query = constructQueryFilterPorn(conditionMap);
        }

        if (MXJudgeUtils.isEmpty(query)) {
            return;
        }
        JSONObject content = constructContentWithTotalNum(query, RECALL_SIZE, null, sortJson);

        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken)) {
            JSONArray sort = parseNextToken(dc.req.nextToken);
            if (null != sort) {
                content.put("search_after", sort);
            }
        }

        String request = content.toJSONString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("VideosOfTheSameAudioRecall search url : %s", request));
            log.debug(String.format("VideosOfTheSameAudioRecall search url : %s", elasticSearchRequest));
        }

        dc.addToESRequestList(
                elasticSearchRequest,
                request,
                this.getName(), "",
                DefineTool.EsType.VIDEO.getTypeName()
        );
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private JSONArray parseNextToken(String nextToken) {
        if (MXJudgeUtils.isEmpty(nextToken)) {
            return null;
        }
        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(nextToken, "|");
        if (3 > tokens.length) {
            return null;
        }

        int token_1 = Integer.parseInt(tokens[0]);
        double token_2 = Double.parseDouble(tokens[1]);
        long token_3 = Long.parseLong(tokens[2]);
        //兼容老板本
        result.add(token_1);
        result.add(token_2);
        result.add(token_3);

        return result;
    }

    private String findMasterAudio(String audioId) {
        String masterAudio = "";
        Map<String, String> subMasterAudioMap = MXDataSource.cache().getBindAudioMapCache(BIND_AUDIO_CACHE_KEY);
        if (MXJudgeUtils.isEmpty(subMasterAudioMap)) {
            subMasterAudioMap = MXDataSource.redis().getBindAudioMap(BIND_AUDIO_REDIS_KEY);
            if (MXJudgeUtils.isEmpty(subMasterAudioMap)) {
                return masterAudio;
            }
            MXDataSource.cache().setBindAudioMapCache(BIND_AUDIO_CACHE_KEY, subMasterAudioMap);
        }
        if (subMasterAudioMap.containsKey(audioId)) {
            masterAudio = subMasterAudioMap.get(audioId);
        } else if (subMasterAudioMap.containsValue(audioId)) {
            masterAudio = audioId;
        }
        return masterAudio;
    }

    private List<String> getBindAudioIds(String masterAudio) {
        String query = "{\"query\":{\"match\":{\"_id\":\"%s\"}}}";
        query = String.format(query, masterAudio);
        List<JSONObject> result = MXDataSource.ES().sendSyncSearchPure(String.format(requestUrlFormatMusic, DefineTool.CategoryEnum.MUSIC_PLAYLIST.getIndex()), query);
        List<String> audioIds = loadSubAudio(result);
        if (!audioIds.contains(masterAudio)) {
            audioIds.add(masterAudio);
        }
        return audioIds;
    }

    private List<String> loadSubAudio(List<JSONObject> result) {
        Set<String> audioIds = new HashSet<>();
        for (JSONObject object : result) {
            if (object.containsKey("sub_tracks")) {
                audioIds.addAll(object.getJSONArray("sub_tracks").toJavaList(String.class));
            }
        }
        return new ArrayList<>(audioIds);
    }
}