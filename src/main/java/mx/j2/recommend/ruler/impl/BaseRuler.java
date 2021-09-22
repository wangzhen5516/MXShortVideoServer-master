package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.component.configurable.base.BaseConfigurableRuler;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.ruler.IRuler;

public abstract class BaseRuler<T extends BaseDataCollection>
        extends BaseConfigurableRuler<T>
        implements IRuler<T> {

    @Override
    public void doWork(T dc) {
        rule(dc);
    }
}
