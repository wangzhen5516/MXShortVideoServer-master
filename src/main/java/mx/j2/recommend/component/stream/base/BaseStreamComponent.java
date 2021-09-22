package mx.j2.recommend.component.stream.base;

import mx.j2.recommend.component.base.BaseComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * 推荐流组件的基类
 */
public abstract class BaseStreamComponent<T extends BaseDataCollection> extends BaseComponent implements IStreamComponent<T> {

    @Override
    public boolean prepare(T dc) {
        return true;
    }
}
