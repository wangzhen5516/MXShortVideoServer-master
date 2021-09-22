package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.thrift.Response;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 数据信息
 */
public class MXDataInfo extends MXBaseDCInfo {
    public MXRecallDataInfo recall;
    public MXResultDataInfo result;
    public MXTempDataInfo temp;
    /**
     * 返回的response。
     */
    public Response response;

    /**
     * 构造函数
     */
    public MXDataInfo() {
        recall = new MXRecallDataInfo();
        result = new MXResultDataInfo();
        temp = new MXTempDataInfo();
        response = new Response();
    }

    @Override
    public void clean() {
        recall.clean();
        result.clean();
        temp.clean();
        response.clear();
    }
}
