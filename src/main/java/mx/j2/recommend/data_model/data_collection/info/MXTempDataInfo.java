package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 临时辅助类数据信息
 */
public class MXTempDataInfo extends MXBaseDCInfo {
    /**
     * 保存 mix 之后的数据，用于打包
     */
    public List<BaseDocument> mixDocumentList;

    /**
     * prepare 组件数据
     */
    public Map<String, Object> prepareResultMap;

    /**
     * 初始化函数
     */
    MXTempDataInfo() {
        mixDocumentList = new ArrayList<>();
        prepareResultMap = new HashMap<>();
    }

    @Override
    public void clean() {
        mixDocumentList.clear();
        prepareResultMap.clear();
    }
}
