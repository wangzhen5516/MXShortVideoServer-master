package mx.j2.recommend.prepare.impl;

import mx.j2.recommend.component.configurable.config.PrepareConfig;
import mx.j2.recommend.component.list.match.IMatch;
import mx.j2.recommend.data_model.BaseUserProfileItem;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.userprofile.base.BaseUserProfileDS;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.prepare.impl.BasePrepare;

import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/9 下午2:26
 * @description 个性化准备基类
 */
public abstract class BaseUserProfilePrepare<R extends BaseUserProfileItem<P>, // result 类型
        P,// 实际要使用的个性化数据类型
        T extends BaseDataCollection>
        extends BasePrepare<T> {
    private static final String KEY_TABLE = "table";
    private static final String KEY_COLUMN = "column";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_TABLE, String.class);
        outConfMap.put(KEY_COLUMN, String.class);
        outConfMap.put(PrepareConfig.KEY_MATCH, IMatch.class);
    }

    @Override
    public void run(T dc) {
        R r = getData(dc);
        setData(dc, r);
    }

    protected String getUserId(T dc) {
        return dc.client.user.uuId;
    }

    abstract R newData();

    private R getData(T dc) {
        R r = newData();

        P profileData = getUserProfileDataFromDS(dc);
        r.setData(profileData);

        IMatch match = getMatch();
        r.setMatch(match);

        return r;
    }

    private void setData(T dc, R result) {
        dc.setPrepareResult(getNameValue(), result);
    }

    private String getNameValue() {
        return config.getName();
    }

    private String getTable() {
        return config.getString(KEY_TABLE);
    }

    private String getColumn() {
        return config.getString(KEY_COLUMN);
    }

    private BaseUserProfileDS getDataSource() {
        return MXDataSource.profileStgTag();
    }

    private P getUserProfileDataFromDS(T dc) {
        String userId = getUserId(dc);
        String table = getTable();
        String column = getColumn();
        return (P) getDataSource().getData(userId, table, column);
    }
}
