package mx.j2.recommend.data_source;

import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Set;

/**
 * @author xiang.zhou
 * @description
 * @date 2020/12/22
 */
public class UserProfileDataSourceTest extends TestCase {

    public static String result;

    public UserProfileDataSourceTest() {
        result = "{\"publisher_id\": [[\"137f98965f2e9e8e7cf0e488a44847d02e\", 3], [\"130f00d2e59d95880f58b95686843ae1ff\", 3], [\"131f4d22dd4c8c54341d8065aa258bff52\", 3], [\"13fdb4867ae04eb1c982eb98df220a2124\", 3]], \"unlike_publisher_id\": [[\"13299ec2f92e959061a77921f231dc1ff0\", 2], [\"135013453014fbc18304486d02450d0f8c\", 2], [\"136b14f6e75e7b3cdd7dfc538f4c41cb9b\", 2], [\"133049d7d82516920cb31838f919cdec26\", 1], [\"136ff0964a17e200a8c3a86c48add1d223\", 1], [\"13a716916b1d6a2c22c38aced2b166304b\", 1], [\"13828753f1fa4ad78defa9fd4665688cb0\", 1], [\"13f0ea9ab162961dad313683de4f6aeaae\", 1], [\"133f0a175bf3ba8ccd37243fdc94d821ee\", 1], [\"13e5dcd32adc10956278ba9302218d2c3a\", 1], [\"13087a6384a90c73758e135ec745b7384d\", 1], [\"14919325483012\", 1], [\"14919953249023\", 1], [\"138ae88429e1af6ca4fa346ded1ae5ffbc\", 1], [\"1306b7363257f5d1ae854c70d2d5c0bdf5\", 1], [\"13d05eb83f78c23f2c9bb0f5bc4f2e3d2d\", 1], [\"13eaae93c2fe6d52bd7054db037b851a44\", 1], [\"1348869e388731e277cb6c09fe3afd21d6\", 1], [\"1372dd16a87a1ed04ad3173223cf44fd66\", 1], [\"13257f34793351d6cbbdd8c7eee18ace14\", 1]], \"desc_tag\": [[\"foryou\", 10], [\"trending\", 8], [\"funny\", 7], [\"comedy\", 5], [\"viral\", 4], [\"fyp\", 3]], \"tags\": [[\"lip-sync\", 10], [\"comedy\", 5], [\"funny\", 4], [\"beautiful-girl\", 3], [\"fashion-style\", 3]]}";
    }

    public void testGetUserTagsScoreFromResult() {
        UserProfileDataSource.getUserTagsScoreFromResult(result);
    }

    public void testGetUserDescTagScoreFromResult() {
        UserProfileDataSource.getUserDescTagScoreFromResult(result);
    }

    public void testGetUserPublisherIdScoreFromResult() {
        UserProfileDataSource.getUserPublisherIdScoreFromResult(result);
    }

    public void testCalculateCos() {
        Set<String> tags = new HashSet<>();
        tags.add("comedy");

        double a = UserProfileDataSource.calculateCos(UserProfileDataSource.getUserTagsScoreFromResult(result), tags);
        double b = UserProfileDataSource.calculateCos(UserProfileDataSource.getUserDescTagScoreFromResult(result), tags);
        System.out.println(a);
        System.out.println(b);
    }

    public void testGetUserProfile() {
        result = "{\"publisher_id\": [[\"14917738626026\", 24], [\"135c8dffaeb75eda2c2914815340b38710\", 5], [\"12115579803885366886444\", 5], [\"13ca035c6e71419f8b40eff398dc488851\", 3]], \"user_ids\": [{\"takaid\": \"\", \"uuid\": \"867c3be5-d936-4f0b-99a5-294fae54a94d15940472\", \"bl\": 0, \"gender\": \"\", \"userid\": \"12114312391635686395452\", \"ts_create\": 0, \"birthday\": \"\", \"lans\": \"\", \"mnc\": [\"5\"], \"fl\": 0, \"email\": \"\", \"fn\": 0, \"manufacturer\": [\"OPPO\"]}], \"unlike_publisher_id\": [[\"14917738626026\", 11], [\"13ab8c644ec307d0e3e2eaa7e4e8e4231b\", 3], [\"12115579803885366886444\", 2], [\"13a31c0e9ac40e7657d99a24083a4c9a17\", 1], [\"130e3079dec8d8c43644d57b0ac9f1b746\", 1]], \"desc_tag\": [[\"mxtakatak\", 26], [\"takataktrending\", 25], [\"foryou\", 22], [\"viral\", 21], [\"love\", 21], [\"trending\", 18], [\"couplegoals\", 15], [\"couples\", 15], [\"acting\", 13], [\"takatakcreator\", 13], [\"featureme\", 12], [\"judwaafam\", 8], [\"mojit\", 8], [\"mojindia\", 7], [\"letsmoj\", 6], [\"shaddym46\", 6], [\"sanufam\", 5], [\"kbye\", 5], [\"like\", 5], [\"sanulove\", 5]], \"tags\": [[\"funny\", 6], [\"comedy\", 6], [\"lip-sync\", 5], [\"beautiful-girl\", 4], [\"trick\", 3]]}";
        String r = UserProfileDataSource.getUserProfile(result);
        System.out.println(r);
    }
}