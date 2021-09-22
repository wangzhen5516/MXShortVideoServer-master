package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 7:58 下午 2021/7/5
 */
public class MultiMapMixer extends BaseListMixer<BaseDataCollection, Map<String, List<BaseDocument>>> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return super.skip(dc) || MXJudgeUtils.isEmpty(getResult(dc));
    }

    @Override
    public void mix(BaseDataCollection dc) {
        mixOneMap(dc, getResult(dc));
    }
}
