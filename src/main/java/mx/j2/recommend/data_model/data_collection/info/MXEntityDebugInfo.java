package mx.j2.recommend.data_model.data_collection.info;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 实体调试信息
 */
public class MXEntityDebugInfo extends MXBaseDCInfo {
    public MXRecallDebugInfo recall;

    MXEntityDebugInfo() {
        recall = new MXRecallDebugInfo();
    }

    @Override
    public void clean() {
        recall.clean();
    }
}
