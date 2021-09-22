package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.UserProfileTag;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.ExposurePoolConf;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * "个性化 boost 池"混入器
 */
public class ProfilePoolMixer extends ExposurePoolMixer {
    private static final String KEY_P_TAG = "p_tag";
    private static final String KEY_CATEGORY = "category";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_P_TAG, String.class);
        outConfMap.put(KEY_CATEGORY, String.class);
    }

    @Override
    public void moveToList(BaseDataCollection dc, List<BaseDocument> toAdd, double ratio, List<BaseDocument> source) {
        // 拿所有的个性化标签
        Set<UserProfileTag> tagSet = getUserProfileTags(dc);

        // 若命中个性化标签则混入
        moveToListOnCondition(toAdd, ratio, source, document -> shouldMix(tagSet, document));
    }

    /**
     * 从 prepare 结果中拿需要的标签数据
     */
    private Set<UserProfileTag> getUserProfileTags(BaseDataCollection dc) {
        Set<UserProfileTag> tagSet = new HashSet<>();
        UserProfileTag tag;

        tag = getTagPrepared(dc, getPTag());
        tagSet.add(tag);

        tag = getTagPrepared(dc, getCategory());
        tagSet.add(tag);

        return tagSet;
    }

    /**
     * 是否应该混入
     */
    private boolean shouldMix(Set<UserProfileTag> tagSet, BaseDocument document) {
        for (UserProfileTag tagIt : tagSet) {
            if (MXJudgeUtils.isNotEmpty(tagIt) && tagIt.matches(document)) {
                return true;
            }
        }
        return false;
    }

    ExposurePoolConf getPoolConf(BaseDataCollection dc) {
        return MXDataSource.profilePoolConf().get(dc.recommendFlow.name);
    }

    private String getPTag() {
        return config.getString(KEY_P_TAG);
    }

    private String getCategory() {
        return config.getString(KEY_CATEGORY);
    }

    /**
     * 从 prepare 结果中拿数据
     */
    private UserProfileTag getTagPrepared(BaseDataCollection dc, String key) {
        return (UserProfileTag) getPrepareResult(dc, key);
    }
}
