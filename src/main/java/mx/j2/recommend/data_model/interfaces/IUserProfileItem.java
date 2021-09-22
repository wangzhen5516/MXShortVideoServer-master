package mx.j2.recommend.data_model.interfaces;

import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/13 下午1:22
 * @description 个性化项目接口
 */
public interface IUserProfileItem {
    /**
     * 个性化特征匹配
     */
    boolean matches(BaseDocument document);
}
