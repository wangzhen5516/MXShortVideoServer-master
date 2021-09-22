package mx.j2.recommend.data_model.data_collection.info;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 客户端信息
 */
public class MXClientInfo extends MXBaseDCInfo {
    public MXRequestInfo request;
    public MXUserInfo user;

    /**
     * 初始化函数
     */
    public MXClientInfo() {
        request = new MXRequestInfo();
        user = new MXUserInfo();
    }

    @Override
    public void clean() {
        request.clean();
        user.clean();
    }
}
