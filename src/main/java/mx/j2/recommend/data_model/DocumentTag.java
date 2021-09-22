package mx.j2.recommend.data_model;

import java.io.Serializable;

/**
 * 解析到文档中的标签结构
 */
public class DocumentTag implements Serializable {
    public float confidence;
    public String parentName;
    public String name;

    @Override
    public String toString() {
        return "{" + confidence + ", " + parentName + ", " + name + "}";
    }
}
