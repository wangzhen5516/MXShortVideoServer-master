package mx.j2.recommend.util;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.UserProfileTag;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.flow.FlowConfigIf;

import java.util.Collection;
import java.util.Map;

/**
 * 上帝审判工具类，只要你愿意，什么都可以往里放
 * <p>
 * TODO 把所有的判断都放这一个类里，不用到处乱找了
 * 注：看在上帝的份上请用 editor-fold 分类归档
 */
public abstract class MXJudgeUtils {

    // <editor-fold desc="Collections & Array & Map">

    public static boolean isEmpty(Object[] array) {
        return MXCollectionUtils.isEmpty(array);
    }

    public static boolean isNotEmpty(Object[] array) {
        return MXCollectionUtils.isNotEmpty(array);
    }

    public static boolean isEmpty(Collection collection) {
        return MXCollectionUtils.isEmpty(collection);
    }

    public static boolean isNotEmpty(Collection collection) {
        return MXCollectionUtils.isNotEmpty(collection);
    }

    public static boolean isEmpty(Map map) {
        return MXCollectionUtils.isEmpty(map);
    }

    public static boolean isNotEmpty(Map map) {
        return MXCollectionUtils.isNotEmpty(map);
    }

    // </editor-fold>

    // <editor-fold desc="String">

    public static boolean isEmpty(CharSequence str) {
        return MXStringUtils.isEmpty(str);
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isBlank(String str) {
        return MXStringUtils.isBlank(str);
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static boolean isNumeric(String str) {
        return MXStringUtils.isNumeric(str);
    }

    // </editor-fold>

    // <editor-fold desc="Other">

    public static boolean isEmpty(BaseDataCollection dc) {
        return dc == null || isEmpty(dc.getResultList());
    }

    public static boolean isEmpty(FlowConfigIf configIf) {
        return configIf == null || isEmpty(configIf.flow) || isEmpty(configIf.range);
    }

    public static boolean isNotEmpty(FlowConfigIf configIf) {
        return !isEmpty(configIf);
    }

    public static boolean isProdEnv() {
        return !isPreEnv() && DefineTool.Env.PROD.confValue.equals(Conf.getEnv());
    }

    public static boolean isPreEnv() {
        return DefineTool.Env.PROD.confValue.equals(Conf.getEnv()) && MXJudgeUtils.isNotEmpty(Conf.getSubEnv());
    }

    public static boolean isDevEnv() {
        return DefineTool.Env.DEV.confValue.equals(Conf.getEnv());
    }

    public static boolean isEmpty(UserProfileTag tag) {
        return tag == null || tag.isEmpty();
    }

    public static boolean isNotEmpty(UserProfileTag tag) {
        return !isEmpty(tag);
    }

    /**
     * @param dc dc
     * @return true 登录了, false 未登录
     */
    public static boolean isLogin(BaseDataCollection dc) {
        return !dc.client.user.userId.equals(dc.client.user.uuId);
    }

    public static boolean isNotLogin(BaseDataCollection dc) {
        return !isLogin(dc);
    }

    // </editor-fold>
}
