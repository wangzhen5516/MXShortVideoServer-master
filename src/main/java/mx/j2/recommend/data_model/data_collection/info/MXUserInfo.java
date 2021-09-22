package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.data_model.UserProfile;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 用户信息
 */
public class MXUserInfo extends MXBaseDCInfo {
    public String userId;
    public String uuId;
    public String adId;
    public boolean isLogined;
    public boolean isHaveMachineID;

    /**
     * 用户个性化数据信息
     */
    public UserProfile profile;

    /**
     * 小流量标记。
     */
    public String userSmallFlowString;

    /**
     * 小流量分组值。
     */
    public int userSmallFlowCode;

    /**
     * 初始化函数
     */
    MXUserInfo() {
        userId = "";
        uuId = "";
        isLogined = false;
        isHaveMachineID = false;
        userSmallFlowString = "";
        profile = new UserProfile();
    }

    @Override
    public void clean() {
        userId = "";
        uuId = "";
        isLogined = false;
        isHaveMachineID = false;
        userSmallFlowString = "";
        profile.clean();
    }
}
