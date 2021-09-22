package mx.j2.recommend.task;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:28 下午 2020/10/15
 */
public class LanguageTask implements Callable<List<BaseDocument>> {

    /**
     * 语言 id
     */
    private final String languageId;

    /**
     * 该 task 用于哪个召回器
     */
    private final String recallName;

    /**
     * 召回数量，写死 1000
     */
    private static final int RECALL_SIZE = 1000;

    /**
     * 语言数据前缀
     */
    private static final String REDIS_KEY_PREFIX = "tophot_language_%s";

    public LanguageTask(String languageId, String recallName) {
        this.languageId = languageId;
        this.recallName = recallName;
    }

    @Override
    public List<BaseDocument> call() throws Exception {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        Map<String, Double> languageCalculateMap = elasticCacheSource.getManualControltCache(
                String.format(REDIS_KEY_PREFIX, languageId),
                RECALL_SIZE);

        if (MXJudgeUtils.isEmpty(languageCalculateMap)) {
            return null;
        }

        return MXDataSource.details().get(languageCalculateMap.keySet(), recallName);
    }
}
