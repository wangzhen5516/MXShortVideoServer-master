package mx.j2.recommend.data_model;

import java.io.Serializable;

/**
 * 解析到文档中的客户端版本信息
 */
public class ClientVersionInfo implements Serializable {

    private int android = -1;// -1 表示该字段无效，要么没有，要么错误
    private String ios;

    public int getAndroid() {
        return android;
    }

    public void setAndroid(int android) {
        this.android = android;
    }

    public String getIos() {
        return ios;
    }

    public void setIos(String ios) {
        this.ios = ios;
    }

    @Override
    public String toString() {
        return "{" + android + ", " + ios + "}";
    }
}
