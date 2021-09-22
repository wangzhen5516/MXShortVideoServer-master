package mx.j2.recommend.component.configurable;

import mx.j2.recommend.data_model.interfaces.IParser;

import java.util.HashMap;
import java.util.Map;

/**
 * 配置映射表解析器，将配置字符串解析成映射表
 * 此过程作为整体配置解析的一个中间步骤，方便下一步解析
 */
public final class ConfigMapParser implements IParser<String, Map<String, String>> {

    @Override
    public Map<String, String> parse(String configString) {
        String[] kvPairs = configString.split(ComponentConfig.Format.KV_PAIR_SEPARATOR);
        String[] kvIt;
        Map<String, String> confMap = new HashMap<>();

        for (String kvPairIt : kvPairs) {
            kvIt = kvPairIt.split(ComponentConfig.Format.KV_SEPARATOR);
            confMap.put(kvIt[0], kvIt[1]);
        }

        return confMap;
    }
}
