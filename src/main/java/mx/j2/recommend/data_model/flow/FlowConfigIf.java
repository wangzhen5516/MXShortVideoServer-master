package mx.j2.recommend.data_model.flow;

/**
 * 接口下的小流量配置
 */
public class FlowConfigIf {
    public String flow;// 小流量详情配置
    public String range;// 小流量区间配置

    public FlowConfigIf(String flow, String range) {
        this.flow = flow;
        this.range = range;
    }

    @Override
    public String toString() {
        return "{flow:" + flow + ",range:" + range + "}";
    }
}
