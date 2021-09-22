package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 发布的第一个视频
 *
 * @author zhiyuan.wang
 * @date 2021/7/9 5:33 下午
 */
public class FirstVideoOfYearRecall extends SearchEngineRecall<OtherDataCollection> {

    private final Logger log = LogManager.getLogger(FirstVideoOfYearRecall.class);
    private JSONArray sortJson;
    private final static int RECALL_FROM = 0;
    private final static int SIZE = 1;
    private final static String INTERFACE_NAME = DefineTool.FlowInterface.MX_FIRST_VIDEO_OF_YEAR_VERSION_1_0.getName();

    public FirstVideoOfYearRecall() {
        init();
    }

    @Override
    public void init() {
        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        sortCore.put(DefineTool.EsKey.ORDER, DefineTool.EsKey.ASC);
        sortCore.put(DefineTool.EsKey.MISSING, "0");
        JSONObject sortObj = new JSONObject();
        sortObj.put(DefineTool.ES.ONLINE_TIME, sortCore);
        sortJson.add(sortObj);
    }

    @Override
    public boolean skip(OtherDataCollection baseDC) {
        return !MXJudgeUtils.isLogin(baseDC);
    }

    @Override
    public void constructRequestURL(OtherDataCollection dc) {
        int hash = dc.req.getResourceId().hashCode() & Integer.MAX_VALUE;
        String indexUrl = String.format(DefineTool.ES.TAKATAK_SIMPLE_TRIGGER, hash % 16);
        String elasticSearchRequest = String.format(DefineTool.ES.SEARCH_FORMAT, indexUrl);

        Map<String, String> condition = new HashMap<>();
        condition.put(dc.req.getResourceType() + "_id", dc.req.getResourceId());
        condition.put(DefineTool.ES.IS_UGC_CONTENT, String.valueOf(true));
        assembleRequest(dc, elasticSearchRequest, condition);

        dc.searchEngineRecallerSet.add(this.getName());
    }


    private void assembleRequest(OtherDataCollection dc, String elasticSearchRequest, Map<String, String> conditionMap) {
        JSONObject query = constructQueryByConditionFilterPrivateVideo(conditionMap);

        if (MXJudgeUtils.isEmpty(query)) {
            return;
        }

        JSONObject content = constructContent(query, RECALL_FROM, SIZE, null, sortJson);
        String request = content.toJSONString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("FirstVideoOfYearRecall search url : %s", request));
            log.debug(String.format("FirstVideoOfYearRecall search url : %s", elasticSearchRequest));
        }

        dc.addToESRequestList(
                elasticSearchRequest,
                request,
                this.getName(), "",
                DefineTool.EsType.VIDEO_NEW.getTypeName());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

}
