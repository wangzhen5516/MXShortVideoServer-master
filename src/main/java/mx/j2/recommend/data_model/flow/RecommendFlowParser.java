package mx.j2.recommend.data_model.flow;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_source.ComponentDataSource;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;

/**
 * 推荐流解析
 *
 * @author zhuowei
 */
@ThreadSafe
public class RecommendFlowParser {

    /**
     * 推荐流解析
     *
     * @param fileName
     * @return 返回全部推荐流，<推荐流name->推荐流>
     */
    @Deprecated
    public static void parseRecommendFlow(String fileName, Map<String, RecommendFlow> result) {
//		Map<String, RecommendFlow> result = new HashMap<String, RecommendFlow>();
        String content = FileTool.readContent(fileName);
        JSONArray jsonArray = JSONArray.parseArray(content);

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject flow = (JSONObject) jsonArray.get(i);
            RecommendFlow rf = parseOneFlow(flow);
            result.put(rf.name, rf);
        }
//		return result;
    }

    /**
     * 解析一个流，抽出这样一个方法，供大家使用
     *
     * @param flow 配置结构
     * @return RecommendFlow
     */
    static RecommendFlow parseOneFlow(final JSONObject flow) {
        RecommendFlow rf = new RecommendFlow();

        String name = (String) flow.get("name");
        rf.name = name;
        String interfaceName = (String) flow.get("interfaceName");
        rf.interfaceName = interfaceName;

        String range = (String) flow.get("range");
        if (MXJudgeUtils.isNotEmpty(range)) {
            String[] tmp = range.split("-");
            rf.start = Integer.parseInt(tmp[0]);
            rf.end = Integer.parseInt(tmp[1]);
        }

        if (flow.containsKey("historyCache")) {
            String historyCache = (String) flow.get("historyCache");
            rf.historyCache = Boolean.parseBoolean(historyCache);
        } else {
            rf.historyCache = false;
        }
        if (flow.containsKey("resultCache")) {
            String resultCache = (String) flow.get("resultCache");
            rf.resultCache = Boolean.parseBoolean(resultCache);
        } else {
            rf.resultCache = false;
        }
        if (flow.containsKey("userProfileCache")) {
            String userProfileCache = (String) flow.get("userProfileCache");
            rf.userProfileCache = Boolean.parseBoolean(userProfileCache);
        } else {
            rf.userProfileCache = false;
        }
        if (flow.containsKey("interfaceResultCache")) {
            String interfaceResultCache = (String) flow.get("interfaceResultCache");
            rf.interfaceResultCache = Boolean.parseBoolean(interfaceResultCache);
        } else {
            rf.userProfileCache = false;
        }
        if (flow.containsKey("predictor")) {
            rf.predictor = flow.getString("predictor");
            ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.PREDICTOR, rf.predictor);
        } else {
            rf.predictor = "";
        }

        if (flow.containsKey("prepare")) {
            JSONArray prepare = (JSONArray) flow.get("prepare");
            for (int k = 0; k < prepare.size(); k++) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.PREPARE, prepare.getString(k));
                rf.prepareList.add(prepare.getString(k));
            }
        }

        JSONArray recall = (JSONArray) flow.get("recall");
        String recallIt;// 源字符串，可能包含配置信息

        for (int k = 0; k < recall.size(); k++) {
            recallIt = recall.getString(k);
            rf.recallList.add(recallIt);

            // TODO-WZD 脏逻辑，先忽略内部召回器
            if (!recallIt.startsWith("Internal")) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.RECALL, recallIt);
            }
        }
        if (flow.containsKey("preRecall")) {
            JSONArray preRecall = (JSONArray) flow.get("preRecall");
            for (int i = 0; i < preRecall.size(); ++i) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.PRERECALL, preRecall.getString(i));
                rf.preRecallList.add(preRecall.getString(i));
            }
        }

        if (flow.containsKey("pool")) {
            JSONArray pool = (JSONArray) flow.get("pool");
            for (int k = 0; k < pool.size(); k++) {
                rf.poolListInFlow.add(pool.getString(k));
            }
        }

        JSONArray cache = (JSONArray) flow.get("cache");
        if (null != cache) {
            for (int k = 0; k < cache.size(); k++) {
                rf.cacheList.add(cache.getString(k));
            }
        }

        JSONArray scorer = (JSONArray) flow.get("scorer");
        if (null != scorer) {
            for (int k = 0; k < scorer.size(); k++) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.SCORER, scorer.getString(k));
                rf.scorerList.add(scorer.getString(k));
            }
        }

        JSONArray mixer = (JSONArray) flow.get("mixer");
        for (int k = 0; k < mixer.size(); k++) {
            ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.MIXER, mixer.getString(k));
            rf.mixerList.add(mixer.getString(k));
        }
        if (flow.containsKey("guaranteeMixer")) {
            JSONArray guaranteeMixer = (JSONArray) flow.get("guaranteeMixer");
            for (int k = 0; k < guaranteeMixer.size(); k++) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.MIXER, guaranteeMixer.getString(k));
                rf.guaranteeMixerList.add(guaranteeMixer.getString(k));
            }
        }
        JSONArray ruler = (JSONArray) flow.get("ruler");
        for (int k = 0; k < ruler.size(); k++) {
            ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.RULER, ruler.getString(k));
            rf.rulerList.add(ruler.getString(k));
        }

        JSONArray preFilter = (JSONArray) flow.get("pre_filter");
        if (preFilter != null) {
            for (int k = 0; k < preFilter.size(); k++) {
                ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.PREFILTER, preFilter.getString(k));
                rf.preFilterList.add(preFilter.getString(k));
            }
        }

        JSONArray filter = (JSONArray) flow.get("filter");
        for (int k = 0; k < filter.size(); k++) {
            ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.FILTER, filter.getString(k));
            rf.filterList.add(filter.getString(k));
        }

        JSONArray cacheForRecall = (JSONArray) flow.get("cacheForRecall");
        if (null != cacheForRecall) {
            for (int k = 0; k < cacheForRecall.size(); k++) {
                rf.cacheForRecallList.add(cacheForRecall.getString(k));
            }
        }

        if (flow.containsKey("matchCondition")) {
            JSONObject jsonObject = flow.getJSONObject("matchCondition");
            if (jsonObject.containsKey("platform")) {
                rf.matchCondition.setPlatfromID(jsonObject.getString("platform"));
            }
        }

        if (flow.containsKey("realType")) {
            JSONArray realType = flow.getJSONArray("realType");
            if (MXCollectionUtils.isNotEmpty(realType)) {
                for (int i = 0; i < realType.size(); ++i) {
                    rf.realType.add(realType.getString(i));
                }
            }
        }

        if (flow.containsKey("requestNum")) {
            String requestNum = flow.getString("requestNum");
            if (MXJudgeUtils.isNotEmpty(requestNum)) {
                rf.requestNum = requestNum;
            }
        }

        rf.ranker = (String) flow.get("ranker");
        ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.RANKER, rf.ranker);

        rf.packer = (String) flow.get("packer");
        if (!rf.packer.contains("Internal")) {// TODO-WZD 脏逻辑，先忽略内部组件
            ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.PACKER, rf.packer);
        }

        rf.fallback = (String) flow.get("fallback");
        ComponentDataSource.INSTANCE.add(IStreamComponent.TypeEnum.FALLBACK, rf.fallback);

        rf.toString = flow.toString();

        return rf;
    }

    public static void main(String[] args) {
    }
}
