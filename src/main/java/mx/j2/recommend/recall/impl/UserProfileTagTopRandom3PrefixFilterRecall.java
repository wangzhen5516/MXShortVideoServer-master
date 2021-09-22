package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;

import java.util.List;

/**
 * @author qiqi
 * @date 2021-01-30 16:14
 */
public class UserProfileTagTopRandom3PrefixFilterRecall extends UserProfileTagTopRandom2Recall {

    private String prefix = "human_tag_in_";

    public UserProfileTagTopRandom3PrefixFilterRecall() {
        TAG_NUM = 3;
    }

    @Override
    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !tag.name.matches(prefix + "[\\w]*"));
    }
}
