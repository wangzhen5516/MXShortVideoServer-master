package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.SHORT_VIDEO;

public abstract class BaseDataSource {
    private final Logger logger = LogManager.getLogger(BaseDataSource.class);

    List<BaseDocument> loadToDocument(BaseDataCollection dc, List<JSONObject> objects) {
        List<BaseDocument> documents = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(objects)) {
            return documents;
        }

        for (JSONObject obj : objects) {
            ShortDocument doc = new ShortDocument().loadJsonObj(obj, SHORT_VIDEO, dc.idToRecallNameMap.get(obj.getString("metadata_id")));
            if (null != doc) {
                documents.add(doc);
            }
        }
        return documents;
    }

    List<BaseDocument> loadToDocument(BaseDataCollection dc, List<JSONObject> objects, IDocumentProcessor processor) {
        List<BaseDocument> documents = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(objects)) {
            return documents;
        }

        for (JSONObject obj : objects) {
            ShortDocument doc = new ShortDocument().loadJsonObj(obj, SHORT_VIDEO, dc.idToRecallNameMap.get(obj.getString("metadata_id")));
            if (null != doc) {
                if (processor != null) {
                    processor.process(doc);
                }
                documents.add(doc);
            }
        }
        return documents;
    }

    String getName() {
        return this.getClass().getSimpleName();
    }

    public BaseDataSource() {
        try {
            onCreate();
            logger.info(getName() + " On-Create init done!");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(getName() + " On-Create init error!");
            logger.error(e.toString());
        }
    }

    /**
     * 数据源构造时的初始化操作
     */
    protected void onCreate() {
    }
}
