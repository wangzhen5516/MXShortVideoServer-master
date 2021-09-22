package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangeWithScoresStragegyCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2021/01/26
 */
public class NewLanguageRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 100;

    private final String REDIS_KEY_FORMAT = "tophot_language";

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.getLanguageList())) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        List<String> languageList = new ArrayList<>(dc.req.getLanguageList());

        if (MXJudgeUtils.isNotEmpty(dc.req.getOldLanguageList())) {
            languageList.removeAll(dc.req.getOldLanguageList());
        }

        if (MXJudgeUtils.isEmpty(languageList)) {
            return;
        }

        String language = languageList.get(0);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", REDIS_KEY_FORMAT, language);
        List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(localCacheKey);
        if (MXJudgeUtils.isEmpty(documents)) {
            ZrevRangeWithScoresStragegyCommand command = new ZrevRangeWithScoresStragegyCommand(
                    String.format("%s_%s", REDIS_KEY_FORMAT, language), RECALL_SIZE);
            Map<String, Double> docWithScores = command.execute();
            if (MXJudgeUtils.isEmpty(docWithScores)) {
                return;
            }
            documents = MXDataSource.details().get(docWithScores.keySet(), this.getName());
            if (MXJudgeUtils.isEmpty(documents)) {
                return;
            }
            for (BaseDocument doc : documents) {
                if (docWithScores.containsKey(doc.id)) {
                    doc.scoreDocument.offlineCalculateScore = docWithScores.get(doc.id);
                }
            }
            documents.sort((o1, o2) -> Double.compare(o2.scoreDocument.offlineCalculateScore, o1.scoreDocument.offlineCalculateScore));
            if (MXJudgeUtils.isNotEmpty(documents)) {
                localCacheDataSource.setTopHotTagDocumentCache(localCacheKey, documents);
            }
        }
        dc.newLanguageDocumentList.addAll(documents);
    }
}
