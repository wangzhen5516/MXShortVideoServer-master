package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.list.check.base.BaseCheck;

/**
 * Check 管理器
 */
public class CheckManager extends BaseComponentManager<BaseCheck> {

    @Override
    IComponent.TypeEnum getComponentType() {
        return IComponent.TypeEnum.CHECK;
    }
}