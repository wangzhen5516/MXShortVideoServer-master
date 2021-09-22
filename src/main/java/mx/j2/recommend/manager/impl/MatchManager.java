package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.list.match.base.BaseMatch;

/**
 * Match 管理器
 */
public class MatchManager extends BaseComponentManager<BaseMatch> {

    @Override
    IComponent.TypeEnum getComponentType() {
        return IComponent.TypeEnum.MATCH;
    }
}