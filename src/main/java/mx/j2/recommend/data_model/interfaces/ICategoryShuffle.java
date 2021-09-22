package mx.j2.recommend.data_model.interfaces;

import java.util.List;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/22 下午3:51
 * @description 按类别打散接口
 * 例：C1,C2,C3,C4,C1,C2,C3,C4...
 */
public interface ICategoryShuffle<E> {
    String CATEGORY_DEFAULT = "default";

    // TODO-WZD 日后应提供一个 default 完全版本
    void run(List<E> list);

    String getCategory(E element);
}
