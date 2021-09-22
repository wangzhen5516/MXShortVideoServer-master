package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 单列表混入器，大多数是这种
 */
public class ListMixer extends BaseListMixer<BaseDataCollection, List<BaseDocument>> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXJudgeUtils.isEmpty(getResult(dc)) || super.skip(dc);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        mixOneList(dc, getResult(dc));
    }
}
