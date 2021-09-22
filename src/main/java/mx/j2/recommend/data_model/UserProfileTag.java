package mx.j2.recommend.data_model;

import mx.j2.recommend.data_source.database.cassandra.UserProfileStrategyTagCA;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * 个性化标签类
 */
public class UserProfileTag extends BaseUserProfileItemPullOnce<Set<UserProfile.Tag>> {
    /**
     * 标签集合
     */
    private Set<UserProfile.Tag> tags;

    public UserProfileTag() {
        tags = new HashSet<>();
    }

    @Override
    public void doPull(String userId, String table) {
        Set<UserProfile.Tag> tags = MXDataSource.profileStgTag().getData(
                userId,
                table,
                UserProfileStrategyTagCA.COLUMN);

        if (MXJudgeUtils.isNotEmpty(tags)) {
            this.tags.clear();
            this.tags.addAll(tags);
        }
    }

    @Override
    public Set<UserProfile.Tag> getData() {
        return tags;
    }

    @Override
    public void setData(Set<UserProfile.Tag> data) {
        if (MXJudgeUtils.isNotEmpty(data)) {
            this.tags.clear();
            this.tags.addAll(data);
        }
    }

    public Set<String> toNameSet() {
        return MXJudgeUtils.isNotEmpty(tags) ? UserProfile.Tag.toNameSet(tags) : new HashSet<>();
    }

    @Override
    public void clean() {
        super.clean();
        tags.clear();
    }

    @Override
    public boolean isEmpty() {
        return MXJudgeUtils.isEmpty(tags);
    }
}
