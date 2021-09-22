package mx.j2.recommend.component.configurable;

import mx.j2.recommend.component.configurable.base.BaseComponentConfig;
import mx.j2.recommend.component.configurable.base.BaseConfigurableComponent;
import mx.j2.recommend.component.configurable.base.IConfigurableComponent;
import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.util.DefineTool;

/**
 * 可配置组件解析器
 *
 * @param <T> 可配置组件类型
 */
public final class ConfigurableComponentParser<T extends BaseConfigurableComponent> implements IParser<String, T> {
    // 组件所在的包名     
    private String packagePrefix;

    public ConfigurableComponentParser(String packagePrefix) {
        this.packagePrefix = packagePrefix;
    }

    @Override
    public T parse(String componentStr) throws Exception {
        // 分割源字符串后的数组, [0] 为组件名字，[1] 为配置信息
        String[] splits = ComponentConfig.Parser.split(componentStr);

        // 生成组件实例
        Class clazz = Class.forName(packagePrefix + splits[0]);
        T cc = (T) clazz.newInstance();

        // 生成配置实例
        BaseComponentConfig config = new ComponentConfigParser(cc).parse(splits[1]);
        cc.setConfig(config);

        // 设置组件全称
        cc.setFullName(componentStr);

        return cc;
    }

    public static void main(String[] args) {
        // 测试可配置组件是否生效只需填写如下信息
        String componentStr = "UserProfileRedisActionV1Mixer{result:user_profile_redis_action_v1,count:0.33,type:in_order,skip:[NoSkip]}";
        String packagePrefix = "mx.j2.recommend.mixer.impl.";

        try {
            IConfigurableComponent<BaseComponentConfig> component = new ConfigurableComponentParser(packagePrefix).parse(componentStr);
            System.out.println(component.getConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
