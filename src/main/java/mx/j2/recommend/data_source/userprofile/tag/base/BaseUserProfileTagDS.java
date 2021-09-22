package mx.j2.recommend.data_source.userprofile.tag.base;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_source.userprofile.base.BaseUserProfileDS;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午3:52
 * @description 个性化标签 CA 数据源基类
 */
public abstract class BaseUserProfileTagDS extends BaseUserProfileDS<Set<UserProfile.Tag>> {

    /**
     * 解析标签集合
     */
    @Override
    protected Set<UserProfile.Tag> parse(String result) {
        Set<UserProfile.Tag> tags = new HashSet<>();

        if (MXJudgeUtils.isEmpty(result)) {
            return tags;
        }

        JSONObject tagMap = JSONObject.parseObject(result);
        if (MXJudgeUtils.isEmpty(tagMap)) {
            return tags;
        }

        for (String tagNameIt : tagMap.keySet()) {
            float tagScoreIt = tagMap.getFloatValue(tagNameIt);
            tags.add(new UserProfile.Tag(tagNameIt, tagScoreIt));
        }

        return tags;
    }
}
