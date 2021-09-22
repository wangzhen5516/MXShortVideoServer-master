package mx.j2.recommend.prepare.impl;

import mx.j2.recommend.component.configurable.base.BaseConfigurablePrepare;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/9 下午2:10
 * @description 准备基类
 */
public abstract class BasePrepare<T extends BaseDataCollection>
        extends BaseConfigurablePrepare<T> {

    @Override
    public void doWork(T dc) {
        run(dc);
    }
}
