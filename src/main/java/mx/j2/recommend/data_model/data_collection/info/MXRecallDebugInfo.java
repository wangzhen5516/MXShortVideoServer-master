package mx.j2.recommend.data_model.data_collection.info;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 召回调试信息
 */
public class MXRecallDebugInfo extends MXBaseDCInfo {
    public String name;// 召回器名字（类名）
    public String result;// 召回器拉链名字
    public String sourceId;// 源视频Id

    MXRecallDebugInfo() {
        name = "";
        result = "";
        sourceId = "";
    }

    @Override
    public void clean() {
        name = "";
        result = "";
        sourceId = "";
    }
}
