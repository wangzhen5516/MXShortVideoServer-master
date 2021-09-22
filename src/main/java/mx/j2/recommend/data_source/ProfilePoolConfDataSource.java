package mx.j2.recommend.data_source;

import mx.j2.recommend.conf.Conf;

/**
 * 个性化池配置数据源
 */
public class ProfilePoolConfDataSource extends BaseExposurePoolConfDataSource {

    @Override
    String getConfPath() {
        return Conf.getProfilePoolConf();
    }
}
