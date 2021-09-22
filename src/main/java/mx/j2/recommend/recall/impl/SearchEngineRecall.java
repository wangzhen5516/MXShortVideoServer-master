package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * 基于搜索引擎的召回，abstract类型，不可以实例化
 *
 * @author zhuowei
 */
public abstract class SearchEngineRecall<T extends BaseDataCollection> extends BaseRecall<T> {

    /**
     * 召回（主方法）
     *
     * @param dc
     */
    @Override
    public void recall(T dc) {
        constructRequestURL(dc);
    }

    public abstract void constructRequestURL(T dc);
}
