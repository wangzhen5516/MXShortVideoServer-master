package mx.j2.recommend.manager;

import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

/**
 * 流组件管理器接口，增加流操作
 *
 * @param <T> 组件类型
 */
public interface IStreamComponentManager<T extends IStreamComponent> extends IComponentManager<T>, ISkip<BaseDataCollection> {
    /**
     * 处理流程
     */
    default void process(BaseDataCollection dc) throws Exception {
        if (!skip(dc)) {
            preProcess(dc);
            inProcess(dc);
            postProcess(dc);
        }
    }

    /**
     * 预处理
     */
    void preProcess(BaseDataCollection dc) throws Exception;

    /**
     * 处理中
     */
    default void inProcess(BaseDataCollection dc) throws Exception {
        List<String> list = list(dc);
        if (MXJudgeUtils.isEmpty(list)) {
            return;
        }

        T componentIt;
        for (String componentNameIt : list) {
            componentIt = getComponentInstance(componentNameIt);
            if (componentIt != null) {
                componentIt.process(dc);
            }
        }
    }

    /**
     * 后处理
     */
    void postProcess(BaseDataCollection dc) throws Exception;

    /**
     * 自检
     */
    void check() throws Exception;

    /**
     * 组件列表
     */
    List<String> list(BaseDataCollection dc);
}
