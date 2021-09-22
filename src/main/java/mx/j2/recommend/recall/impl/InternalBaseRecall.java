package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.recall.InternalRecall;

/**
 * @author DuoZhao
 */
public abstract class InternalBaseRecall implements InternalRecall {

    /**
     * 数据召回接口
     *
     * @param dc 一次请求的数据集合
     */
    @Override
    public void procRecall(InternalDataCollection dc) {
        dc.moduleStartTime = System.nanoTime();
        recall(dc);
        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, this.getName());
    }

    @Override
    public abstract void recall(InternalDataCollection dc);

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void doWork(BaseDataCollection dc) {
        // do nothing
    }

    @Override
    public boolean skip(Object data) {
        return false;
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        return true;
    }
}
