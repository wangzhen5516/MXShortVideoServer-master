package mx.j2.recommend.data_model;

import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * 个性化作者类
 */
public class UserProfilePublisher extends BaseUserProfileItemPullOnce<Set<UserProfile.Publisher>> {
    /**
     * 作者集合
     */
    private Set<UserProfile.Publisher> publishers;

    public UserProfilePublisher() {
        publishers = new HashSet<>();
    }

    @Override
    public void doPull(String userId, String table) {

    }

    @Override
    public Set<UserProfile.Publisher> getData() {
        return publishers;
    }

    @Override
    public void setData(Set<UserProfile.Publisher> publishers) {
        if (MXJudgeUtils.isNotEmpty(publishers)) {
            this.publishers.clear();
            this.publishers.addAll(publishers);
        }
    }

    public Set<String> toIdSet() {
        return MXJudgeUtils.isNotEmpty(publishers) ? UserProfile.Publisher.toIdSet(publishers) : new HashSet<>();
    }

    @Override
    public void clean() {
        super.clean();
        publishers.clear();
    }

    @Override
    public boolean isEmpty() {
        return MXJudgeUtils.isEmpty(publishers);
    }
}
