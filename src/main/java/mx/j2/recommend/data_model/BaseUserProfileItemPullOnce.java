package mx.j2.recommend.data_model;

/**
 * 请求内只拉一次版本
 *
 * @param <T> 个性化数据类型
 */
abstract class BaseUserProfileItemPullOnce<T> extends BaseUserProfileItem<T> {
    /**
     * 本次请求内是否已经拉取过一次了
     */
    private boolean hasPullOnce;

    boolean pull(String userId, String accessToken) {
        if (!hasPullOnce) {
            hasPullOnce = true;
            doPull(userId, accessToken);
        }

        return isNotEmpty();
    }

    /**
     * 去远端拉数据
     */
    @Deprecated
    abstract void doPull(String userId, String accessToken);

    @Override
    public void clean() {
        super.clean();
        hasPullOnce = false;
    }
}
