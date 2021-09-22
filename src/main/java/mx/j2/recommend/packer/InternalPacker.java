package mx.j2.recommend.packer;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;

/**
 * 程序打包封装ui展现的接口
 *
 * @author DuoZhao
 *
 */
public interface InternalPacker {

    /**
     * 结果打包，根据ui需要装配打包元素
     */
    void pack(InternalDataCollection dc, OtherDataCollection otherDc);

    /**
     * 获取打包程序的name
     *
     */
    String getName();
}
