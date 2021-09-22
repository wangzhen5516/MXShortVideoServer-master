package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.DefineTool;

@Getter
@Setter
public class BadgeDocument extends BaseDocument {

    //        cols.add("max_days");
    //        cols.add("max_weeks");
    //        cols.add("total_days");
    private int maxDays;
    private int maxWeeks;
    private int totalDays;
    private long timestamp;

    public BadgeDocument() {
        this.id = "1";
    }
}
