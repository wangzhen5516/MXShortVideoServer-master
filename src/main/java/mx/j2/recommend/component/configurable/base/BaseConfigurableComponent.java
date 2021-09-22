package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.base.BaseComponent;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @param <C> 配置类型
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午3:54
 * @description 可配置组件基类
 * @see BaseComponentConfig
 */
public abstract class BaseConfigurableComponent<C> extends BaseComponent implements IConfigurableComponent<C> {
    /**
     * 配置信息
     */
    protected C config;

    /**
     * 可配置组件的名字 = 类名 + 配置
     */
    private String fullName;

    @Override
    public C getConfig() {
        return config;
    }

    @Override
    public void setConfig(C config) throws Exception {
        this.config = config;
    }

    @Override
    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    @Override
    public String getFullName() {
        return MXJudgeUtils.isNotEmpty(fullName) ? fullName : getName();
    }
}
