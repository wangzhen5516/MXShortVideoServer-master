package mx.j2.recommend.mixer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UgcLowLevelMixParamDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.ExposurePoolConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 曝光池混入器
 */
public class ExposurePoolMixer extends ListMixer {

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
    }

    @Override
    @Trace(dispatcher = true)
    public void mix(BaseDataCollection dc) {
        ExposurePoolConf poolConf = getPoolConf(dc);
        if (poolConf == null) {
            return;
        }

        List<BaseDocument> result = getResult(dc);
        double mixParam = UgcLowLevelMixParamDataSource.getMixParam();
        double rate = poolConf.rate;

        // 按比例稀释混入量
        if (result.size() < mixParam) {
            rate = result.size() / mixParam * rate;
        }

        double numberToMix = rate * (double) dc.req.num;
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToList(dc, toAdd, numberToMix, result);
        addDocsToMixDocument(dc, toAdd);
    }

    /**
     * 获取池子配置
     */
    ExposurePoolConf getPoolConf(BaseDataCollection dc) {
        return MXDataSource.exposurePoolConf().get(dc.recommendFlow.name);
    }
}
