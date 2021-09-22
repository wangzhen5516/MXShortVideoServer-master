package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 用户个性标签过滤器
 *
 * @see mx.j2.recommend.recall.impl.UserProfileTagTopRecall
 * @see mx.j2.recommend.mixer.impl.UserProfileTagTopMixer
 */
@SuppressWarnings("unused")
public class UserProfileTagTopFilter extends BaseFilter {
    private static final int TAG_NUM = 3;// 只需要前 3 个标签

    private Random random = new Random();

    @Override
    public boolean prepare(BaseDataCollection dc) {
        List<UserProfile.Tag> tagList = MXDataSource.profileTag().getTags(dc);
        if (MXJudgeUtils.isEmpty(tagList)) {
            return false;
        }

        /*
         * 过滤、排序并截断，只保留最多 3 个小于 0 且分数最低的标签
         */

        tagList = tagList.stream().filter(tag -> tag.score < 0).collect(Collectors.toList());
        tagList.sort(Comparator.comparing(o -> o.score));
        tagList = tagList.subList(0, Math.min(TAG_NUM, tagList.size()));

        /*
         * 以映射的方式存起来
         */

        Map<String, Float> lowScoreMap = new HashMap<>();

        for (UserProfile.Tag tag : tagList) {
            lowScoreMap.put(tag.name, tag.score);
        }

        dc.client.user.profile.setLowScoreMap(lowScoreMap);

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        Map<String, Float> lowScoreMap = dc.client.user.profile.getLowScoreMap();
        if (MXJudgeUtils.isEmpty(lowScoreMap)) {
            return false;
        }

        // 保留和视频相关的标签
        Set<String> tempSet = new HashSet<>(lowScoreMap.keySet());
        tempSet.retainAll(doc.mlTags);

        for (String tag : tempSet) {
            if ((random.nextFloat() - 1) > lowScoreMap.get(tag)) {
                return true;
            }
        }

        return false;
    }
}
