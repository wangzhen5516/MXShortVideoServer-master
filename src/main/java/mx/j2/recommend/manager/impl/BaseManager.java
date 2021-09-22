package mx.j2.recommend.manager.impl;

import mx.j2.recommend.manager.IManager;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/4/19 下午5:50
 * @description 管理器基类
 */
public abstract class BaseManager implements IManager {

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
