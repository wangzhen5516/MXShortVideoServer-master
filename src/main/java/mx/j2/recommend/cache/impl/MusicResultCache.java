package mx.j2.recommend.cache.impl;

import mx.j2.recommend.manager.MXDataSource;

/**
 * 音乐结果缓存类
 */
@SuppressWarnings("unused")
public class MusicResultCache extends BaseResourceIdResultCache {

    public MusicResultCache() {
        super(s -> MXDataSource.cache().getMusicCache(s), (s, results) -> {
            MXDataSource.cache().setMusicCache(s, results);
            return null;
        });
    }
}
