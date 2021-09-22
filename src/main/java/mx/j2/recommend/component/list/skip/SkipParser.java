package mx.j2.recommend.component.list.skip;

import mx.j2.recommend.component.configurable.ListConfigParser;
import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.manager.MXManager;

import java.util.ArrayList;
import java.util.List;

/**
 * skip 配置解析器
 * <p>
 * 例：
 * [classname1|classname2]
 * <p>
 * Use ListConfigParser instead
 *
 * @see ListConfigParser
 */
@Deprecated
public final class SkipParser implements IParser<String, List<ISkip>> {

    @Override
    public List<ISkip> parse(String configStr) throws Exception {
        // 检查合法性
        if (!configStr.startsWith(ComponentConfig.Format.ARRAY_BEGIN)
                || !configStr.endsWith(ComponentConfig.Format.ARRAY_END)) {
            throw new Exception("Format error.");
        }

        // 掐头去尾去掉 []
        configStr = configStr.substring(1, configStr.length() - 1);

        // 切分所有的名字
        String[] skipNames = configStr.split(ComponentConfig.Format.ARRAY_CONTENT_SEPARATOR);

        // 去 skip 池子拿实例填充列表
        ISkip skipIt;
        List<ISkip> skips = new ArrayList<>();
        for (String skipNameIt : skipNames) {
            skipIt = MXManager.skip().getComponentInstance(skipNameIt);

            if (skipIt == null) {
                throw new Exception("Class " + skipNameIt + " not found.");
            }

            skips.add(skipIt);
        }

        return skips;
    }
}
