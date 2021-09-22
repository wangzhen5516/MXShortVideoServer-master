package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.util.DefineTool;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.NullStatus;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 工具类数据信息
 */
public class MXUtilInfo extends MXBaseDCInfo {
    /**
     * 表示缓存结果是否可用
     */
    public DefineTool.Cache.CacheStatus cacheStatus;

    /**
     * 当前要进行的缓存操作
     */
    public DefineTool.Cache.CacheOperationEnum cacheOperation;

    /**
     * 初始化函数
     */
    public MXUtilInfo() {
        cacheStatus = NullStatus;
        cacheOperation = DefineTool.Cache.CacheOperationEnum.NONE;
    }

    @Override
    public void clean() {
        cacheStatus = NullStatus;
        cacheOperation = DefineTool.Cache.CacheOperationEnum.NONE;
    }
}
