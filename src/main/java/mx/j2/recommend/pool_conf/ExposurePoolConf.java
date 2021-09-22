package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSONArray;

/**
 * 曝光池配置
 */
public class ExposurePoolConf {
    public int recallSize;
    public double rate;
    public String esIndex;
    public String description;
    public JSONArray sortField;

    ExposurePoolConf() {

    }

    ExposurePoolConf(ExposurePoolConf other) {
        this.esIndex = other.esIndex;
        this.description = other.description;
        this.rate = other.rate;
        this.recallSize = other.recallSize;
        this.sortField = other.sortField;// 这块是浅拷贝，不过没关系，只在解析时用，会重新解析该字段
    }

    public ExposurePoolConf copy() {
        return new ExposurePoolConf(this);
    }

    @Override
    public String toString() {
        return "ExposurePoolConf{" +
                "poolIndex='" + esIndex + '\'' +
                ", recallSize=" + recallSize +
                ", sortField='" + sortField + '\'' +
                ", rate=" + rate +
                ", description='" + description + '\'' +
                '}';
    }
}
