package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.DocCategoryShuffleSimple;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.ICategoryShuffle;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 按种类打散单列表混入器
 */
public class CategoriesListMixer extends ListMixer {
    // "类别打散器"
    private ICategoryShuffle<BaseDocument> categoryShuffle;

    public CategoriesListMixer() {
        categoryShuffle = new DocCategoryShuffleSimple();
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        categoryShuffle.run(getResult(dc));
        return true;
    }
}
