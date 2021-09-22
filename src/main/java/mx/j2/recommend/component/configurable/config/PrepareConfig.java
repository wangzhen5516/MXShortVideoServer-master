package mx.j2.recommend.component.configurable.config;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.base.BaseStreamComponentConfig;

import java.util.Map;

/**
 * 准备器配置类
 *
 * @param <T> 数据类型
 */
public final class PrepareConfig<T> extends BaseStreamComponentConfig<T> {

    public PrepareConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        super(confMap);
    }
}
