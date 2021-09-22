package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 多列表混入器，支持多个列表的召回结果，用 Map 实现
 */
public class MultiListMixer extends BaseListMixer<BaseDataCollection, Map<String, List<BaseDocument>>> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return super.skip(dc) || MXJudgeUtils.isEmpty(getResult(dc));
    }

    @Override
    public void mix(BaseDataCollection dc) {
        for (List<BaseDocument> listIt : getResult(dc).values()) {
            mixOneList(dc, listIt);
        }
    }
}
