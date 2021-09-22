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
public class CardDocument extends BaseDocument {
    Logger logger = LogManager.getLogger(CardDocument.class);
    private String cardName;
    private String hashTag;
    private int order;

    public CardDocument loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName) {
        this.loadJsonObj(source, category, recallName, this);
        this.id = source.getString("id");
        this.cardName = source.getString("card_name");
        this.hashTag = source.getString("hash_tag");
        this.order = source.getIntValue("order");
        if (checkQuality()) {
            logger.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.id);
    }
}
