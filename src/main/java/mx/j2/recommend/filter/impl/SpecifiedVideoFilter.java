package mx.j2.recommend.filter.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.FileTool;

import java.util.List;


//只用于主版接口
public class SpecifiedVideoFilter extends BaseFilter {
    List<String> ids;

    public SpecifiedVideoFilter() {
        String content = FileTool.readContent(Conf.getNeedFilterVideoConfPath());
        JSONObject videoIDConf = JSONObject.parseObject(content);
        if (null != videoIDConf.getJSONArray(Conf.getEnv())) {
            ids = videoIDConf.getJSONArray(Conf.getEnv()).toJavaList(String.class);
        }
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if(ids.contains(doc.id)){
            return true;
        }
        return false;
    }

    private List<String> getNeedFilterVideoIDs(){
        return ids;
    }
}
