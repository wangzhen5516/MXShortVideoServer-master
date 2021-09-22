package mx.j2.recommend.data_model;

/**
 * 个性化性别类
 */
public class UserProfileGender extends BaseUserProfileItemPullOnce<UserProfile.Gender> {
    private UserProfile.Gender gender;

    public UserProfileGender() {
        gender = UserProfile.Gender.UNKNOWN;
    }

    @Override
    public void doPull(String userId, String table) {

    }

    @Override
    public UserProfile.Gender getData() {
        return gender;
    }

    @Override
    public void setData(UserProfile.Gender gender) {
        this.gender = gender;
    }

    @Override
    public void clean() {
        super.clean();
        gender = UserProfile.Gender.UNKNOWN;
    }

    @Override
    public boolean isEmpty() {
        return UserProfile.Gender.UNKNOWN.equals(gender);
    }
}
