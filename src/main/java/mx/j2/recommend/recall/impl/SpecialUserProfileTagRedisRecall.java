package mx.j2.recommend.recall.impl;

import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpecialUserProfileTagRedisRecall extends BaseRecall<FeedDataCollection> {
    private static final String TABLE_NAME = "up_ml_tag_60d_v1";
    private static final String TAG_KEY = "tag";
    private static final String REDIS_KEY_KEY = "redis_key";

    @Override
    public boolean skip(FeedDataCollection dc) {
        String currentTag = config.getString(TAG_KEY);
        List<UserProfile.Tag> tags;
        if (MXJudgeUtils.isNotEmpty(dc.userLongTagSet)) {
            tags = new ArrayList<>(dc.userLongTagSet);
        } else {
            dc.tagTableName = TABLE_NAME;
            tags = MXDataSource.profileTagV2().getTags(dc);
        }

        if (MXJudgeUtils.isEmpty(tags)) {
            return true;
        }
        for (UserProfile.Tag tag : tags) {
            if (currentTag.equals(tag.name)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void recall(FeedDataCollection dc) {
        String currentTag = config.getString(TAG_KEY);
        String key = config.getString(REDIS_KEY_KEY);
        List<BaseDocument> documentList = MXDataSource.cache().getPubgDocCache(key);
        if (MXJudgeUtils.isNotEmpty(documentList)) {
            addResult(dc, documentList);
            dc.syncSearchResultSizeMap.put(this.getName() + "_" + currentTag, documentList.size());
            dc.resultFromMap.put(this.getName() + "_" + currentTag, DefineTool.RecallFrom.LOCAL.getName());
            return;
        }
        List<String> idList = MXDataSource.redis().getZrevRangeStrageyList(key, 0, -1);
        if (MXJudgeUtils.isEmpty(idList)) {
            return;
        }
        documentList = MXDataSource.details().get(idList);
        if (MXJudgeUtils.isEmpty(documentList)) {
            return;
        }
        MXDataSource.cache().setPubgDocCache(key, documentList);
        addResult(dc, documentList);
        dc.syncSearchResultSizeMap.put(this.getName() + "_" + currentTag, documentList.size());
        dc.resultFromMap.put(this.getName() + "_" + currentTag, DefineTool.RecallFrom.REDIS.getName());
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(TAG_KEY, String.class);
        outConfMap.put(REDIS_KEY_KEY, String.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
    }
}
