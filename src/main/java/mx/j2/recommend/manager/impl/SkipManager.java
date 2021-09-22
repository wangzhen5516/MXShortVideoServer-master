package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.list.skip.base.BaseDCSkip;

/**
 * Skip 管理器
 */
public class SkipManager extends BaseComponentManager<BaseDCSkip> {

    @Override
    IComponent.TypeEnum getComponentType() {
        return IComponent.TypeEnum.SKIP;
    }
}