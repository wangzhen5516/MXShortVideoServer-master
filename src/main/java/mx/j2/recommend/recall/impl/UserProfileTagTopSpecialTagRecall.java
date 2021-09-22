package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 使用特殊的标签集合召回
 */
@SuppressWarnings("unused")
@Deprecated
public class UserProfileTagTopSpecialTagRecall extends UserProfileTagTopRandom2Recall {

    private final Set<String> SPECIAL_TAGS = new HashSet<String>() {
        {
            add("funny");
            add("movie");
            add("bts");
            add("pubg");
            add("gongzuo");

            // 语言 tag
            add("language_Gujarati");
            add("language_Malayalam");
            add("language_Bengali");
            add("language_Kannada");
            add("language_Punjabi");
            add("language_Telugu");
            add("language_Tamil");
            add("language_Hindi");
        }
    };

    public UserProfileTagTopSpecialTagRecall() {
        TAG_NUM = 2;// 只使用其中的两个
    }

    /**
     * 只使用写死的特殊的标签
     */
    @Override
    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !SPECIAL_TAGS.contains(tag.name));
    }
}
