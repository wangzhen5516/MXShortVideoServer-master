package mx.j2.recommend.component.configurable;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/18 下午7:32
 * @description
 *
 * 配置值信息对，配置的中间存储格式，用于解析出最终的值
 */
public final class ConfigValuePair {
    public String content;// 值的内容
    public Class type;// 值的类型

    public ConfigValuePair(String content, Class type) {
        this.content = content;
        this.type = type;
    }
}
