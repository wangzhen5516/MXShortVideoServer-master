package mx.j2.recommend.data_model;

import mx.j2.recommend.component.list.match.IMatch;
import mx.j2.recommend.data_model.data_collection.info.MXBaseDCInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IUserProfileItem;

/**
 * 个性化项目基类
 *
 * @param <T> 个性化数据类型
 */
public abstract class BaseUserProfileItem<T>
        extends MXBaseDCInfo implements IUserProfileItem {
    /**
     * 匹配实例
     */
    private IMatch<BaseUserProfileItem, BaseDocument> match;

    /**
     * 远端拉取
     *
     * @param userId      用户 ID
     * @param accessToken CA table or REDIS key
     * @return true 当前过程结束后是有数据的
     */
    @Deprecated
    abstract boolean pull(String userId, String accessToken);

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    public abstract boolean isEmpty();

    @Override
    public void clean() {
        match = null;
    }

    public void setMatch(IMatch match) {
        this.match = match;
    }

    public abstract void setData(T data);

    public abstract T getData();

    /**
     * 匹配
     */
    @Override
    public boolean matches(BaseDocument document) {
        return match != null && match.matches(this, document);
    }
}
