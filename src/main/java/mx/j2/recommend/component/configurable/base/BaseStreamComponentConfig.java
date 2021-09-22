package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.ObjectConfigParser;
import mx.j2.recommend.component.list.match.IMatch;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.component.list.skip.SkipParser;
import mx.j2.recommend.manager.MXManager;

import java.util.List;
import java.util.Map;

/**
 * 流组件配置基类
 */
public abstract class BaseStreamComponentConfig<T> extends BaseComponentConfig {
    public static final String KEY_SKIP = "skip";
    public static final String KEY_MATCH = "match";

    public BaseStreamComponentConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        super(confMap);
    }

    /**
     * 跳过集合
     */
    List<ISkip<T>> getSkips() {
        return (List<ISkip<T>>) getObject(KEY_SKIP);
    }

    /**
     * 匹配项
     */
    IMatch getMatch() {
        return (IMatch) getObject(KEY_MATCH);
    }

    /**
     * 解析 skips
     */
    @Override
    protected Object parseValue(ConfigValuePair valuePair) throws Exception {
        if (valuePair.type.equals(ISkip.class)) {
            return new SkipParser().parse(valuePair.content);
        } else if (valuePair.type.equals(IMatch.class)) {
            return new ObjectConfigParser(MXManager.match()).parseObject(valuePair.content);
        }

        return super.parseValue(valuePair);
    }
}
