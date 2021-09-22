package mx.j2.recommend.data_source.base;

import mx.j2.recommend.data_source.BaseDataSource;
import mx.j2.recommend.data_source.database.cassandra.base.ICassandraDB;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午3:52
 * @description CA 数据源基类
 */
public abstract class BaseCassandraDS<I, R> extends BaseDataSource {
    private ICassandraDB cassandra;

    @Override
    protected void onCreate() {
        cassandra = newCassandra();
    }

    protected abstract ICassandraDB newCassandra();

    protected abstract String getKeySpace();

    public R getData(I inParam, String table, String column) {
        String queryStr = buildQuery(inParam, table);
        String queryResultStr = query(queryStr, column);
        return parse(queryResultStr);
    }

    protected abstract String buildQuery(I inParam, String table);

    protected String query(String query, String column) {
        return cassandra != null ? cassandra.query(query, column) : "";
    }

    protected abstract R parse(String result);
}
