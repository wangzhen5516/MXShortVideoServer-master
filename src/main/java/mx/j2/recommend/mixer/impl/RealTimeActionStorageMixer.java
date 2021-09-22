package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.PrivateAccountZAddCommand;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 非真正混入器，只是为了实现如下功能：
 * <p>
 * 混入后把剩余的文档放到 trending 用的 realtime 列表里
 */
public class RealTimeActionStorageMixer extends ListMixer {
    private static final String KEY_FORMAT = "format";// redis key format
    private static final String KEY_TOTAL = "total";// redis 里保存的总数量
    private static final String KEY_EXPIRE = "expire";// 过期时间

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
        outConfMap.put(MixerConfig.KEY_COUNT, Float.class);
        outConfMap.put(KEY_FORMAT, String.class);
        outConfMap.put(KEY_TOTAL, Integer.class);
        outConfMap.put(KEY_EXPIRE, Integer.class);
    }

    /**
     * 移动到 trending redis
     */
    @Override
    public void mix(BaseDataCollection dc) {
        final String redisKey = String.format(getFormat(), dc.req.userInfo.getUuid());
        Set<String> idList = new HashSet<>();
        List<BaseDocument> result = getResult(dc);

        result.subList(0, Math.min((int) getCount(dc), result.size())).forEach(x -> {
            if (x != null && MXJudgeUtils.isNotEmpty(x.id)) {
                idList.add(x.id);
            }
        });

        new PrivateAccountZAddCommand(redisKey, idList, getTotalConfig(), getExpireConfig()).execute();
    }

    /**
     * 获取格式化 redis key
     * 这块为了兼容包含":"，配置的时候用"-"代替，这里再转换一下
     */
    private String getFormat() {
        String formatConfig = getFormatConfig();
        return formatConfig.replace("-", ":");
    }

    /**
     * 获取"redis key format"配置
     */
    private String getFormatConfig() {
        return config.getString(KEY_FORMAT);
    }

    /**
     * 获取"总存入数量"配置
     */
    private int getTotalConfig() {
        return config.getInt(KEY_TOTAL);
    }

    /**
     * 获取"过期时间"配置
     */
    private int getExpireConfig() {
        return config.getInt(KEY_EXPIRE);
    }
}
