package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/18 下午5:18
 * @description
 */
public class StrategyTagPoolRecallWeightedShuffleNumber5Score30 extends StrategyTagPoolRecallWeightedShuffle {
    public StrategyTagPoolRecallWeightedShuffleNumber5Score30() {
        USE_TAG_NUMBER = 5;
    }

    @Override
    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !isQualified(tag));
        List<UserProfile.Tag> tempTags = tags.stream().filter(tag -> tag.score > 30).collect(Collectors.toList());
        tags.clear();
        tags.addAll(tempTags);
    }
}
