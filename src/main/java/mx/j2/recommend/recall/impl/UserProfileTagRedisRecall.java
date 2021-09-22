package mx.j2.recommend.recall.impl;

import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author qiqi
 * @date 2021-05-27 11:34
 */
public class UserProfileTagRedisRecall extends UserProfileTagRecall {

    private static final String REDIS_KEY = "redis_key";
    private static final String NUM = "num";
    private static final String TABLE = "table";


    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
        outConfMap.put(REDIS_KEY, String.class);
        outConfMap.put(NUM, Integer.class);
        outConfMap.put(TABLE, String.class);
    }

    @Override
    public void doRecall(BaseDataCollection dc, List<String> userTags) {
        if (MXCollectionUtils.isEmpty(userTags)) {
            return;
        }
        Map<String, List<BaseDocument>> resMap = new HashMap<>(userTags.size());
        for (String tagIt : userTags) {
            if (MXStringUtils.isBlank(tagIt)) {
                continue;
            }
            List<BaseDocument> list = MXDataSource.tagTop().getVideosByTag(getRedisKey(), tagIt);
            if (MXJudgeUtils.isNotEmpty(list)) {
                List<BaseDocument> copyList = new ArrayList<>(list.size());
                DefineTool.deepClone(list, copyList);
                resMap.put(tagIt, copyList);
            }
        }
        if (MXCollectionUtils.isNotEmpty(resMap)) {
            resMap.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
            setResult(dc, resMap);
        }
    }

    /**
     * 获取redisKey
     *
     * @return
     */
    private String getRedisKey() {
        return config.getString(REDIS_KEY);
    }

    /**
     * 获取配置的 tag 数量，使用几个 tag 召回
     */
    @Override
    public int getNum() {
        return config.getInt(NUM);
    }

    /**
     * 获取 tag CA table 配置，从哪个表召回 tag
     */
    @Override
    public String getTable() {
        return config.getString(TABLE);
    }
}
