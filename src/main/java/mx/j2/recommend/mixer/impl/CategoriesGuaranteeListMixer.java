package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.DocCategoryShuffleSimple;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.ICategoryShuffle;

import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 按种类打散保底单列表混入器
 */
public class CategoriesGuaranteeListMixer extends GuaranteeListMixer {
    // "类别打散器"
    private ICategoryShuffle<BaseDocument> categoryShuffle;

    public CategoriesGuaranteeListMixer() {
        categoryShuffle = new DocCategoryShuffleSimple();
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        categoryShuffle.run(getResult(dc));
        return true;
    }
}
