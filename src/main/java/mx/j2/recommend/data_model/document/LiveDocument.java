package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


/**
 * @author qiqi
 * @date 2021-03-16 20:51
 */
@Data
public class LiveDocument extends BaseDocument {

    Logger logger = LogManager.getLogger(LiveDocument.class);
    /**
     * 直播间所在州名
     */
    private String liveStateName;

    /**
     * 直播流Id
     */
    private String streamId;
    /**
     * 需要置顶的指标数据
     */
    private String liveOrder;
    /**
     * 该直播间是否是白名单（1：是，0：否）
     */
    private int liveWhiteList;
    /**
     * 该直播间的语言列表
     */
    private List<String> liveLanguageLists;
    /**
     * 直播类型
     */
    private String liveCategory;
    /**
     * 本人打的分为了后面排序（白名单关注4/普通关注3/白名单2/普通1）
     */
    private double liveScore;

    private boolean isFollow;

    public LiveDocument loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName, BaseDataCollection dc) {
        this.loadJsonObj(source, category, recallName, this);
        if (MXCollectionUtils.isEmpty(source)) {
            return null;
        }
        if (source.containsKey("stream_id")) {
            this.streamId = source.getString("stream_id");
            this.nextTokenMap.put(dc.req.interfaceName, source.getString("stream_id"));
        }
        if (source.containsKey("state")) {
            this.liveStateName = source.getString("state");
        }
        if (source.containsKey("order")) {
            this.liveOrder = source.getString("order");
        }
        if (source.containsKey("whitelist")) {
            this.liveWhiteList = source.getIntValue("whitelist");
        }
        if (source.containsKey("language")) {
            String languageLists = source.getString("language");
            try {
                if (MXStringUtils.isNotBlank(languageLists)) {
                    this.liveLanguageLists = JSONArray.parseArray(languageLists).toJavaList(String.class);
                }
            } catch (Exception e) {
                logger.error("parse languageList error", e);
            }
        }
        /*白名单初始化为2分，普通为1分*/
        if (this.liveWhiteList == 1) {
            this.liveScore = 2;
        } else {
            this.liveScore = 1;
        }
        if (checkQuality()) {
            logger.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.streamId);
    }

    @Override
    public String toString() {
        return "LiveDocument{" +
                "streamId='" + streamId + '\'' +
                ", stateName='" + liveStateName + '\'' +
                ", publisherId='" + super.publisher_id + '\'' +
                ", liveScore='" + liveScore + '\'' +
                ", liveLanguage='" + liveLanguageLists + '\'' +
                '}';
    }
}
