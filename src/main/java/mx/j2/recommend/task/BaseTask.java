package mx.j2.recommend.task;

import mx.j2.recommend.util.bean.BloomInfo;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:09 下午 2020/11/6
 */
public abstract class BaseTask {

    TaskExecutor executor;

    public BaseTask (TaskExecutor executor) {
        this.executor = executor;
    }

    /**
     * 回调
     */
    abstract void callback(BloomInfo bloomInfo);

    String getName() {
        return this.getClass().getSimpleName();
    }
}
