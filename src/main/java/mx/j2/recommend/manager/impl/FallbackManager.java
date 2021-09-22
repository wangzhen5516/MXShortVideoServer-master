package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.fallback.impl.BaseFallback;

import java.util.Collections;
import java.util.List;

/**
 * 数据（Video）保底处理器，目前只有 Feed 流支持保底
 *
 * <p>
 * 触发条件
 * 1 timeout
 * 2 rate limit
 * 3 主接口（如 hot）返回空的 Response
 */
@SuppressWarnings("unused")
public class FallbackManager extends BaseStreamComponentManager<BaseFallback> {

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.FALLBACK;
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return Collections.singletonList(dc.recommendFlow.fallback);
    }
}
