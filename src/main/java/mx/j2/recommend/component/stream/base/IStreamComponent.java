package mx.j2.recommend.component.stream.base;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * 推荐流组件接口
 */
public interface IStreamComponent<T extends BaseDataCollection> extends IComponent, ISkip<T> {

    /**
     * 处理接口，是推荐流组件就有处理流程
     */
    default void process(T dc) {
        dc.moduleStartTime = System.nanoTime();

        if (!skip(dc) && prepare(dc)) {
            doWork(dc);
        }

        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, this.getName());
    }

    /**
     * 真正执行业务逻辑任务
     */
    void doWork(T dc);

    /**
     * 执行任务前的准备工作
     *
     * @param dc dc
     * @return true 表示准备工作完成并可以执行业务逻辑了；false 表示准备失败并没有必要执行业务逻辑
     */
    boolean prepare(T dc);
}
