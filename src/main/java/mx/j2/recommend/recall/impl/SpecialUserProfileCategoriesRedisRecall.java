package mx.j2.recommend.recall.impl;

import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;

/**
 * 用户个性化
 * @author zhongren.li
 */

public class SpecialUserProfileCategoriesRedisRecall extends BaseRecall<FeedDataCollection> {
    private static final String UP_TABLE_NAME = "up_table";
    private static final String CATEGORIES_KEY = "categories";
    private static final String REDIS_PREFIX = "redis_prefix";
    private static final String THRESHOLD = "threshold";
    private static final int RECALL_SIZE_LIMIT = 400;

    @Override
    public boolean skip(FeedDataCollection dc) {
        String currentTag = config.getString(CATEGORIES_KEY);
        if (MXJudgeUtils.isEmpty(currentTag)) {
            return true;
        }

        List<UserProfile.Tag> categories;
        if (MXJudgeUtils.isNotEmpty(dc.userLongCategorySet)) {
            categories = new ArrayList<>(dc.userLongCategorySet);
        } else {
            dc.tagTableName = config.getString(UP_TABLE_NAME);
            categories = MXDataSource.profileTagV2().getCategories(dc);
        }

        if (MXJudgeUtils.isEmpty(categories)) {
            return true;
        }

        List<String> categoriesList = getCategoryList(currentTag);
        for (UserProfile.Tag c : categories) {
            if (categoriesList.contains(c.name)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void recall(FeedDataCollection dc) {
        String currentTag = config.getString(CATEGORIES_KEY);
        List<String> retainAll = getRelatedCategories(dc, currentTag);
        if (MXJudgeUtils.isEmpty(retainAll)) {
            return;
        }

        Map<String, List<BaseDocument>> cToDocMap = new HashMap<>(16);
        String keyPrefix = config.getString(REDIS_PREFIX);
        for (String category : retainAll) {
            String key = String.format("%s_%s", keyPrefix, category);

            List<BaseDocument> documentList = MXDataSource.cache().getPubgDocCache(key);
            if (MXJudgeUtils.isNotEmpty(documentList)) {
                cToDocMap.put(category, documentList);
                recordSize(dc, category, documentList.size(), DefineTool.RecallFrom.LOCAL.getName());
                continue;
            }

            List<String> idList = MXDataSource.redis().getZrevRangeStrageyList(key, 0, RECALL_SIZE_LIMIT);
            if (MXJudgeUtils.isEmpty(idList)) {
                continue;
            }
            documentList = MXDataSource.details().get(idList);
            if (MXJudgeUtils.isEmpty(documentList)) {
                continue;
            }
            MXDataSource.cache().setPubgDocCache(key, documentList);
            cToDocMap.put(category, documentList);
            recordSize(dc, category, documentList.size(), DefineTool.RecallFrom.REDIS.getName());
        }

        setResult(dc, cToDocMap);
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(THRESHOLD, Double.class);
        outConfMap.put(UP_TABLE_NAME, String.class);
        outConfMap.put(CATEGORIES_KEY, String.class);
        outConfMap.put(REDIS_PREFIX, String.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
    }

    private List<String> getCategoryList(String str) {
        String[] array = str.replace("[", "").replace("]", "").split("\\|");
        return Arrays.asList(array);
    }

    private List<String> getRelatedCategories(FeedDataCollection dc, String str) {
        List<String> categories = getCategoryList(str);

        List<String> result = new ArrayList<>();
        double threshold = config.getDouble(THRESHOLD);
        for (UserProfile.Tag c : dc.userLongCategorySet) {
            if (categories.contains(c.name)) {
                if (Double.compare(c.score, threshold) > 0) {
                    result.add(c.name);
                }
            }
        }
        return result;
    }

    private void recordSize (BaseDataCollection dc, String category, int size, String from) {
        String key = String.format("%s_%s", this.getName(), category);
        dc.syncSearchResultSizeMap.put(key, size);
        dc.resultFromMap.put(key, from);
    }
}
