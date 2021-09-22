package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Getter
@Setter
public class CardListItemDocument extends BaseDocument {
    private static Logger log = LogManager.getLogger(CardListItemDocument.class);
    //先写死，，
    private String discoveryPageTabId = "5bf68b89b8dc9fce0bfb567750f69100";
    int order;
    int platformType;
    int status;

    public CardListItemDocument loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName) {
        this.loadJsonObj(source, category, recallName, this);
        this.id = source.getString("card_id");
        this.order = source.getInteger("order");
        this.platformType = source.getInteger("platform_type");
        this.status = source.getInteger("status");

        if (checkQuality()) {
            log.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.id);
    }
}
