package mx.j2.recommend.data_model;

import mx.j2.recommend.data_model.data_collection.info.MXBaseDCInfo;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 用户画像
 */
public class UserProfile extends MXBaseDCInfo {
    /**
     * 标签已设置标志位
     */
    private static final int SET_FLAG_TAG = 1;

    /**
     * "已拉取并设置"标记
     */
    private int setFlags = 0;

    /**
     * 标签信息
     */
    private List<Tag> tags = new ArrayList<>();

    /**
     * 个性化标签集合
     */
    public UserProfileTags profileTags;

    /**
     * tag 分数小于 0 的 map
     */
    private Map<String, Float> lowScoreMap;

    /**
     * 当前用户性别
     */
    public String gender;

    public UserProfile() {
        profileTags = new UserProfileTags();
        gender = "";
    }

    /**
     * 标签是否已设置
     */
    public boolean isTagSet() {
        return exists(setFlags, SET_FLAG_TAG);
    }

    /**
     * 设置"标签已设置"标志
     */
    private void setIsTagSet() {
        setFlags |= SET_FLAG_TAG;
    }

    /**
     * 获取当前的标签信息
     */
    public List<Tag> getTags() {
        return tags;
    }

    /**
     * 设置标签信息
     * 一旦调用过，之后只走缓存，不再网络请求
     */
    public void setTags(List<Tag> tags) {
        // 已经设置过了，以后走缓存
        setIsTagSet();

        if (MXJudgeUtils.isNotEmpty(tags)) {
            this.tags.addAll(tags);
        }
    }

    /**
     * 实际上本地有没有拉到有效的标签
     */
    public boolean hasTags() {
        return MXJudgeUtils.isNotEmpty(this.tags);
    }

    /**
     * 获取低分数段的映射
     */
    public Map<String, Float> getLowScoreMap() {
        return lowScoreMap;
    }

    /**
     * 设置低分数段 [-1,0] 的映射
     */
    public void setLowScoreMap(Map<String, Float> map) {
        this.lowScoreMap = map;
    }

    /**
     * 判断某个标志位是否已设置
     */
    private boolean exists(int flags, int flag) {
        return (flags & flag) != 0;
    }

    /**
     * 清空画像内容
     */
    @Override
    public void clean() {
        setFlags = 0;
        tags.clear();
        profileTags.clean();
        gender = "";

        if (lowScoreMap != null) {
            lowScoreMap.clear();
            lowScoreMap = null;
        }
    }

    /**
     * 标签
     */
    public static class Tag {
        /**
         * 标签名
         */
        public String name;

        /**
         * 对标签的喜爱程度分
         * [-1,1]，-1 表示非常讨厌，1 表示非常喜欢
         */
        public Float score;

        public Tag(String name, float score) {
            this.name = name;
            this.score = score;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Tag) {
                Tag other = (Tag) obj;
                return other.name.equals(this.name);
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            if (MXStringUtils.isNotEmpty(this.name)) {
                return this.name.hashCode();
            }
            return super.hashCode();
        }

        @Override
        public String toString() {
            return "(" + this.name + "," + this.score + ")";
        }

        /**
         * 转换为名字集合
         */
        static Set<String> toNameSet(Set<Tag> tags) {
            return tags.stream().map(tag -> tag.name).collect(Collectors.toSet());
        }

        /**
         * 标签类型
         */
        public enum TypeEnum {
            BOOST_POOL,
            CATEGORY,
            LONGTERMTAG,
        }
    }

    /**
     * 作者
     */
    public static class Publisher {
        /**
         * id
         */
        public String id;

        /**
         * 对标签的喜爱程度分
         * [-1,1]，-1 表示非常讨厌，1 表示非常喜欢
         */
        public Float score;

        public Publisher(String id, float score) {
            this.id = id;
            this.score = score;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Publisher) {
                Publisher other = (Publisher) obj;
                return other.id.equals(this.id);
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            if (MXStringUtils.isNotEmpty(this.id)) {
                return this.id.hashCode();
            }
            return super.hashCode();
        }

        @Override
        public String toString() {
            return "(" + this.id + "," + this.score + ")";
        }

        /**
         * 转换为 id 集合
         */
        static Set<String> toIdSet(Set<Publisher> tags) {
            return tags.stream().map(tag -> tag.id).collect(Collectors.toSet());
        }
    }

    /**
     * 性别
     */
    public static class Gender {
        static final Gender UNKNOWN = new Gender(DefineTool.GenderEnum.UNKNOWN);

        public DefineTool.GenderEnum gender;

        Gender(DefineTool.GenderEnum gender) {
            this.gender = gender;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Gender) {
                Gender other = (Gender) obj;
                return other.gender.equals(this.gender);
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return gender.getName();
        }
    }
}
