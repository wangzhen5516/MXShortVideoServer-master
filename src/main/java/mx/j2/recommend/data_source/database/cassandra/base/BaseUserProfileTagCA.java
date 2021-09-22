package mx.j2.recommend.data_source.database.cassandra.base;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:22
 * @description 标签 CA 基类
 */
public abstract class BaseUserProfileTagCA extends BaseUserProfileCA {
    public static final String COLUMN = "tag_profile";

    public BaseUserProfileTagCA(String keySpace) {
        super(keySpace);
    }
}
