package mx.j2.recommend.recall;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;

import java.io.IOException;

/**
 * 内部数据召回接口
 *
 * @author DuoZhao
 */

public interface InternalRecall extends IStreamComponent {
    /**
     * 数据召回接口
     *
     * @param dc 一次请求的数据集合
     */
    void recall(InternalDataCollection dc) throws IOException;

    void procRecall(InternalDataCollection dc);

    /**
     * 获取rankmodel的名字
     */
    String getName();
}
