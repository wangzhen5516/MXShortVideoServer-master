package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 保底单列表混入器
 */
public class GuaranteeListMixer extends ListMixer {

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_TYPE, String.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return resultIsEnough(dc) || super.skip(dc);
    }

    /**
     * 重写混入方式
     */
    @Override
    void mixOneList(BaseDataCollection dc, List<BaseDocument> list) {
        if (getType().equals(MixerConfig.TypeEnum.RANDOM)) {// 随机混入
            Collections.shuffle(list);
        }

        for (BaseDocument doc : list) {
            addOneDocToMixDocument(dc, doc);

            if (resultIsEnough(dc)) {
                break;
            }
        }
    }
}
