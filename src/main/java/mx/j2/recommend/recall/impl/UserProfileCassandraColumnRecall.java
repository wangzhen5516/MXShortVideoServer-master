package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.UserStrategyTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Map;

/**
 * @author qiqi
 * @date 2021-06-09 16:03
 */
public class UserProfileCassandraColumnRecall extends UserProfileCassandraRecall {

    private static final String COLUMN = "column";
    private static final String TABLE = "table";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(COLUMN, String.class);
    }

    private String getColumn() {
        return config.getString(COLUMN);
    }

    private String getBackUpData() {
        String table = config.getString(TABLE);
        UserStrategyTagDataSource userStrategyTagDataSource = MXDataSource.profileTagV2();
        return userStrategyTagDataSource.getStrategyOutPutFromCassandraById("no_user_profile", table, getColumn());
    }

    @Override
    public String getResultFromCa(BaseDataCollection baseDc) {
        UserStrategyTagDataSource userStrategyTagDataSource = MXDataSource.profileTagV2();
        String table = config.getString(TABLE);
        String result = userStrategyTagDataSource.getStrategyOutPutFromCassandraById(BloomUtil.getUuid(baseDc), table, getColumn());
        if (MXStringUtils.isBlank(result)) {
            result = getBackUpData();
        }
        return result;
    }
}
