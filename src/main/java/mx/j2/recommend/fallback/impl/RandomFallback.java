package mx.j2.recommend.fallback.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.FallbackDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

/**
 * 保底召回——随机
 */
@SuppressWarnings("unused")
public class RandomFallback extends BaseFallback<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void fallback(BaseDataCollection dc) {
        List<BaseDocument> resultList = FallbackDataSource.INSTANCE.getRandomFallback(dc.req.num);

        if (MXJudgeUtils.isNotEmpty(resultList)) {
            dc.fallbackList.addAll(resultList);
        }

        dc.resultFromMap.put(this.getName(), "Fallback");
    }
}
