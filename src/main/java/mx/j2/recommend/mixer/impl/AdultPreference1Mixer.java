package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static mx.j2.recommend.util.DefineTool.GenderEnum.FEMALE;

/**
 * @author qiqi
 * @date 2021-03-11 11:23
 */
public class AdultPreference1Mixer extends BaseMixer<BaseDataCollection> {
    private Logger logger = LogManager.getLogger(AdultPreference1Mixer.class);
    private static final String TIME_FORMAT = "HH:mm:ss";
    double ratio = 0.2;
    int size = 2;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXCollectionUtils.isEmpty(dc.adultPreferenceDocumentList);
    }

    @Override
    public void mix(BaseDataCollection dc) {

        String curHour = getCurHour();
        /*获取小时*/
        if (MXStringUtils.isNotBlank(curHour)) {
            int hour = Integer.parseInt(curHour);
            if (23 <= hour || hour < 5) {
                List<BaseDocument> toAdd1 = new ArrayList<>();
                moveToList(dc, toAdd1, ratio * dc.req.num, dc.adultPreferenceDocumentList);
                addDocsToMixDocument(dc, toAdd1);
            }
        }
        /*非女性首次获取*/
        getGender(dc);
        if (FEMALE.getName().equals(dc.client.user.profile.gender) || MXStringUtils.isNotBlank(dc.req.getNextToken())) {
            return;
        }
        List<BaseDocument> docLists = dc.adultPreferenceDocumentList;
        if (docLists.size() > size) {
            docLists = docLists.subList(0, size);
        }
        List<BaseDocument> toAdd = new ArrayList<>(docLists);
        addDocsToMixDocumentHead(dc, toAdd);
    }

    private String getCurHour() {
        try {
            long currentTime = System.currentTimeMillis();
            SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_FORMAT);
            dateFormat.setTimeZone(TimeZone.getTimeZone(DefineTool.TimeZoneEnum.INDIA.timeZone));
            String nowString = dateFormat.format(new Date(currentTime));
            String[] now = nowString.split(":");
            return now[0];
        } catch (Exception e) {
            logger.error("get curHour error", e);
        }
        return null;
    }
}
