package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.MXStringUtils;
import org.springframework.beans.BeanUtils;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * 流量池配置
 *
 * @author zhongren.li
 */
@NotThreadSafe
@Setter
@Getter
public class PoolConf {

    public String poolIndex;

    public JSONArray poolLevel;

    public JSONArray userLevel;

    public JSONArray ignoreFilter;

    public double percentage;

    public int poolRecallSize;

    public int priority;

    public String poolDescription;

    public boolean isTophotHistory;

    public String sortField;
    public JSONArray sortFieldNew;

    public JSONArray generalTags;
    public JSONArray nicheTags;

    public boolean isBuildFlowContent;


    /**
     * 构造函数
     */
    public PoolConf() {
        poolIndex = "";
        poolLevel = new JSONArray();
        userLevel = new JSONArray();
        ignoreFilter = new JSONArray();
        percentage = 0;
        poolRecallSize = 0;
        poolDescription = "";
        priority = 0;
        isTophotHistory = false;
        sortField = "";
        sortFieldNew = null;
        isBuildFlowContent = true;
        generalTags = new JSONArray();
        nicheTags = new JSONArray();
    }

    /**
     * 推荐流
     */
    public PoolConf(PoolConf other) {
        BeanUtils.copyProperties(other, this);
    }

    public PoolConf deepCopy() {
        return new PoolConf(this);
    }

    @Override
    public String toString() {
        return "PoolConf{" +
                "poolIndex='" + poolIndex + '\'' +
                ", poolLevel=" + poolLevel +
                ", userLevel=" + userLevel +
                ", ignoreFilter=" + ignoreFilter +
                ", percentage=" + percentage +
                ", poolRecallSize=" + poolRecallSize +
                ", priority=" + priority +
                ", poolDescription='" + poolDescription + '\'' +
                ", isTophotHistory=" + isTophotHistory +
                ", sortField='" + sortField + '\'' +
                ", sortFieldNew='" + sortFieldNew + '\'' +
                ", isBuildFlowContent='" + isBuildFlowContent + '\'' +
                '}';
    }
}
