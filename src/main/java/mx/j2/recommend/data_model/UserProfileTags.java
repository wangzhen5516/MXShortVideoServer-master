package mx.j2.recommend.data_model;

import mx.j2.recommend.data_model.data_collection.info.MXBaseDCInfo;
import mx.j2.recommend.data_source.database.cassandra.UserProfileStrategyTagCA;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * 用户个性化标签集合
 */
public class UserProfileTags extends MXBaseDCInfo {
    /**
     * 个性化 boost 池标签标志
     */
    private static final int SET_FLAG_BOOST_POOL_TAG = 0x0001;

    /**
     * "已拉取并设置"标记，请求内只拉取一次，至于是否拉取成功不管
     */
    private int setFlags;

    /**
     * "个性化 Boost 池"标签集
     */
    private Set<String> boostPoolTags;

    /**
     * 用户大类别
     */
    public UserProfileTag category;

    /**
     * 长期用户数据，给ProfileTagHindiFilter使用
     */
    public UserProfileTag longTermTag;


    UserProfileTags() {
        setFlags = 0;
        boostPoolTags = new HashSet<>();
        category = new UserProfileTag();
        longTermTag = new UserProfileTag();
    }

    @Override
    public void clean() {
        setFlags = 0;
        boostPoolTags.clear();
        category.clean();
        longTermTag.clean();
    }

    /**
     * 拉取个性化标签入口
     *
     * @param type        标签类型
     * @param userId      用户 ID
     * @param accessToken 数据库表或 redis key
     */
    public boolean pull(UserProfile.Tag.TypeEnum type, String userId, String accessToken) {
        switch (type) {
            case BOOST_POOL:
                return pullBoostPoolTag(userId, accessToken);
            case CATEGORY:
                return category.pull(userId, accessToken);
            case LONGTERMTAG:
                return longTermTag.pull(userId, accessToken);
            default:
                return false;
        }
    }

    //<editor-fold description="Boost Pool">
    private boolean pullBoostPoolTag(String userId, String table) {
        // 如果还没有拉取过
        if (!isSetBoostPoolTag()) {
            Set<UserProfile.Tag> tags = MXDataSource.profileStgTag().getData(
                    userId,
                    table,
                    UserProfileStrategyTagCA.COLUMN);

            Set<String> profileTagSet = null;

            if (MXJudgeUtils.isNotEmpty(tags)) {
                profileTagSet = UserProfile.Tag.toNameSet(tags);
            }

            setBoostPoolTags(profileTagSet);
        }

        return MXJudgeUtils.isNotEmpty(getBoostPoolTags());
    }

    private void setBoostPoolTags(Set<String> tags) {
        // 已经设置过了，以后走缓存
        setBoostPoolTagIsSet();

        if (MXJudgeUtils.isNotEmpty(tags)) {
            boostPoolTags.addAll(tags);
        }
    }

    public Set<String> getBoostPoolTags() {
        return boostPoolTags;
    }

    /**
     * 标签是否已设置
     */
    private boolean isSetBoostPoolTag() {
        return exists(setFlags, SET_FLAG_BOOST_POOL_TAG);
    }

    /**
     * 设置"标签已设置"标志
     */
    private void setBoostPoolTagIsSet() {
        setFlags |= SET_FLAG_BOOST_POOL_TAG;
    }

    //</editor-fold>

    /**
     * 判断某个标志位是否已设置
     */
    private boolean exists(int flags, int flag) {
        return (flags & flag) != 0;
    }
}
