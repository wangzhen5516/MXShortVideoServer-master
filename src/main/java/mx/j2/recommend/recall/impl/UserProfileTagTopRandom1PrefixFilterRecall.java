package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.UserProfile;
import java.util.List;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/2/4 下午12:32
 * @description
 */
public class UserProfileTagTopRandom1PrefixFilterRecall extends UserProfileTagTopRandom2Recall {

    private String HUMAN_PREFIX = "human_tag_in_";
    private String AUDIO_PREFIX = "audio_";
    public static String[] SELECTED_TAG_LIST= {
            "food",
            "movie",
            "lifehack",
            "dog",
            "pet",
            "pubg",
            "goodthing",
            "travel",
            "gongzuo",
            "baby",
            "wwe",
            "makeup",
            "paint",
            "car",
            "tech",
            "edu",
            "cricket",
            "adventure",
            "cat",
            "wildlife",
            "science",
            "bts",
            "diy",
            "ipl",
            "nail",
            "yoga",
            "engineer",
            "yogagirl",
            "football",
            "skiing",
            "kpop",
            "japan",
            "military"};

    public UserProfileTagTopRandom1PrefixFilterRecall() {
        TAG_NUM = 1;
    }

    @Override
    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !isQualified(tag));
    }

    private boolean isQualified(UserProfile.Tag tag){
        if( tag.name.matches(HUMAN_PREFIX + "[\\w]*") ||tag.name.matches(AUDIO_PREFIX + "[\\w]*")){
            return true;
        }
        for(String selectedTag:SELECTED_TAG_LIST){
            if(selectedTag.equals(tag.name)){
                return true;
            }
        }
        return false;
    }
}
