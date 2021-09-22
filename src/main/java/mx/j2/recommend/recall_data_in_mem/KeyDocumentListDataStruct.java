package mx.j2.recommend.recall_data_in_mem;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.util.MXJudgeUtils;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyDocumentListDataStruct implements Serializable {
    private String key;
    private final Map<String, List<BaseDocument>> keyAndDocumentListMap;

    public KeyDocumentListDataStruct(String key) {
        this.key = key;
        this.keyAndDocumentListMap = new ConcurrentHashMap<>();
    }

    public void setDocumentListByKey(String listKey, List<BaseDocument> documentList) {
        if (MXJudgeUtils.isNotEmpty(listKey) && documentList != null) {
            keyAndDocumentListMap.put(listKey, documentList);
        }
    }

    public List<BaseDocument> getDocumentListByKey(String listKey) {
        if (keyAndDocumentListMap.containsKey(listKey)) {
            List<BaseDocument> docs = keyAndDocumentListMap.get(listKey);
            List<BaseDocument> cloneDocs = new ArrayList<>(docs.size());

            deepClone(docs, cloneDocs);
            return cloneDocs;
        }
        return Collections.emptyList();
    }

    private void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        source.forEach(doc -> {
            BaseDocument bdoc = new ShortDocument();
            if (null != bdoc) {
                BeanUtils.copyProperties(doc, bdoc);
                if (null != doc) {
                    bdoc.recallName = doc.recallName;
                }
                target.add(bdoc);
            }
        });
    }
}
