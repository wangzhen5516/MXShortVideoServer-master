package mx.j2.recommend.scorer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_model.flow.LanguageNameToIdFlowParser;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.isFollowUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author qiqi
 * @date 2021-04-01 11:52
 */
public class LiveManualScore extends BaseScorer<BaseDataCollection> {

    Logger logger = LogManager.getLogger(LiveManualScore.class);
    private static final String OTHER_STATE = "others";

    private static final HashMap<String, List<String>> STATE_MAP = new HashMap<String, List<String>>() {
        {
            put("TamilNadu", Arrays.asList("Tamil", "Hindi", "English"));
            put("AndhraPradesh", Arrays.asList("Telugu", "Hindi", "English"));
            put("Telangana", Arrays.asList("Telugu", "Hindi", "English"));
            put("Kerala", Arrays.asList("Malayalam", "Tamil", "Hindi", "English"));
            put("Karnataka", Arrays.asList("Kannada", "Hindi", "English"));
            put("Maharashtra", Arrays.asList("Marathi", "Hindi", "English"));
            put("WestBengal", Arrays.asList("Bengali", "Hindi", "English"));
            put("Assam", Arrays.asList("Bengali", "Hindi", "English"));
            put("Gujarat", Arrays.asList("Gujarati", "Hindi", "English"));
            put("Punjab", Arrays.asList("Punjabi", "Hindi", "English"));
            put("others", Arrays.asList("Hindi", "English"));
        }
    };

    @Override
    public boolean skip(BaseDataCollection data) {
        return MXCollectionUtils.isEmpty(data.liveDocumentList) || !MXJudgeUtils.isLogin(data);
    }

    @Override
    public void score(BaseDataCollection dc) {
        if (!"mx_live_follow_version_1_0".equals(dc.req.interfaceName)) {
            followScorer(dc);
        }
        List<String> reqLanguageId = new ArrayList<>();
        String state = null;
        if (dc.req != null && MXCollectionUtils.isNotEmpty(dc.req.languageList)) {
            reqLanguageId = dc.req.languageList;
        }
        if (dc.req != null && dc.req.location != null && MXStringUtils.isNotBlank(dc.req.location.getState())) {
            state = dc.req.location.getState();
        }
        List<String> reqLanguageName = langIdToName(reqLanguageId);
        List<LiveDocument> deleteList = new ArrayList<>();
        for (BaseDocument doc : dc.liveDocumentList) {
            LiveDocument document = (LiveDocument) doc;
            if (document == null) {
                continue;
            }
            /*关注的数据不参与语言过滤*/
            List<String> docLanguage = document.getLiveLanguageLists();

            /*关注的没语言也没state需要处理*/
            /*判断是否有交集*/
            if (MXCollectionUtils.isNotEmpty(docLanguage)) {
                List<String> res = new ArrayList<>(docLanguage);
                res.retainAll(reqLanguageName);
                if (res.size() > 0) {
                    document.setLiveScore(document.getLiveScore() + 1);
                    continue;
                }
                List<String> stateToLanguage = new ArrayList<>();
                if (STATE_MAP.containsKey(state)) {
                    stateToLanguage = STATE_MAP.get(state);
                } else {
                    stateToLanguage = STATE_MAP.get(OTHER_STATE);
                }
                List<String> res2 = new ArrayList<>(docLanguage);
                res2.retainAll(stateToLanguage);
                if (res2.size() == 0 && !document.isFollow()) {
                    /*live的语言和state对应的语言列表没有交集的需要过滤*/
                    deleteList.add(document);
                } else {
                    int size = stateToLanguage.size();
                    for (int i = 0; i < size; ++i) {
                        if (docLanguage.contains(stateToLanguage.get(i))) {
                            document.setLiveScore(document.getLiveScore() + 0.1 * (size - i));
                        }
                    }
                }
            }
        }
        dc.liveDocumentList.removeAll(deleteList);
    }

    /**
     * 将请求中的语言Id转为Name(ex:en->english)
     *
     * @param languageList
     * @return
     */
    private List<String> langIdToName(List<String> languageList) {
        List<String> languageNames = new ArrayList<>();
        Map<String, String> languageMap = LanguageNameToIdFlowParser.parse();
        if (MXCollectionUtils.isEmpty(languageMap)) {
            return languageNames;
        }
        for (Map.Entry<String, String> res : languageMap.entrySet()) {
            if (languageList.contains(res.getValue())) {
                languageNames.add(res.getKey());
            }
        }
        return languageNames;
    }

    /**
     * 为关注的直播间进行打分
     *
     * @param dc
     */
    private void followScorer(BaseDataCollection dc) {
        if (MXCollectionUtils.isEmpty(dc.liveDocumentList)) {
            return;
        }
        String userId = dc.req.getUserInfo().getUserId();
        StringBuilder builder = new StringBuilder();
        List<LiveDocument> lockList = new ArrayList<>();
        for (int i = 0; i < dc.liveDocumentList.size(); ++i) {
            LiveDocument doc = dc.liveDocumentList.get(i);
            if (MXStringUtils.isNotBlank(doc.getLiveOrder())) {
                lockList.add(doc);
            }
            if (MXStringUtils.isNotBlank(doc.publisher_id)) {
                if (i == dc.liveDocumentList.size() - 1) {
                    builder.append(doc.publisher_id);
                } else {
                    builder.append(doc.publisher_id);
                    builder.append(",");
                }
            }
        }
        dc.liveLockDocumentList.addAll(lockList);
        dc.liveDocumentList.removeAll(lockList);
        List<String> followers = null;
        try {
            followers = isFollowUtil.getFollowedIds(userId, builder.toString());
        } catch (Exception e) {
            logger.error("getFollowedIds is error");
        }
        if (MXCollectionUtils.isNotEmpty(followers)) {
            for (LiveDocument doc : dc.liveDocumentList) {
                if (doc != null && followers.contains(doc.publisher_id)) {
                    if (doc.getLiveWhiteList() == 1) {
                        doc.setLiveScore(4);
                    } else {
                        doc.setLiveScore(3);
                    }
                    doc.setFollow(true);
                }
            }
        }
    }
}
