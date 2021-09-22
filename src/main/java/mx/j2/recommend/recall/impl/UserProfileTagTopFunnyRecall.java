package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2021/01/23
 */

public class UserProfileTagTopFunnyRecall extends BaseRecall<BaseDataCollection> {
    private static final int TAG_NUM = 2;
    private static final String NEED_TAG_NAME = "funny";
    private static final String REDIS_PREFIX = "tophot_ml_tag_";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getTags(baseDc);

        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }

        try {
            tags = tags.stream().filter(tag -> tag.score > 0).collect(Collectors.toList());
            List<String> tagStringList = getTags(tags);
            if (MXJudgeUtils.isEmpty(tagStringList)){
                return;
            }
            if (!tagStringList.contains(NEED_TAG_NAME)) {
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<BaseDocument> list = MXDataSource.tagTop().getVideosByTag(REDIS_PREFIX, NEED_TAG_NAME, getName());
        if (MXJudgeUtils.isNotEmpty(list)) {
            baseDc.funnyList.addAll(list);
        }
        baseDc.syncSearchResultSizeMap.put(this.getName(),list.size());
    }

    private List<String> getTags(List<UserProfile.Tag> tags) {
        List<String> list = new ArrayList<>();
        tags.forEach(tag -> {
            list.add(tag.name);
        });
        return new ArrayList<>(list);
    }

    protected void processTags(List<UserProfile.Tag> tags) {}
}
