package mx.j2.recommend.data_model;

/**
 * Created by zhangxuejian on 2018/1/10.
 *
 * 整个推荐过程的数据集合，打包到一个类中，所有跟当次推荐有关的数据都在这里
 */
public interface DataCollection {
    /**
     *
     * 获取DataCollection的名字
     *
     */
    String getName();
}
