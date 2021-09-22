package mx.j2.recommend.manager;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;

/**
 * @author DuoZhao
 */
public interface InternalManager {

    /**
     * 获取Manager名字
     */
    String getName();

    void process(InternalDataCollection dc, OtherDataCollection otherDc);

    /**
     * 需要的数据源准备完毕通知回调
     */
    default void onDataSourcePrepared() {}
}
