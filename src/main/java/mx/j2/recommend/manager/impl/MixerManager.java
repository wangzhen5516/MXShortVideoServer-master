package mx.j2.recommend.manager.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.mixer.impl.BaseMixer;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.commons.lang.math.JVMRandom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * mixer 管理器，管理所有的 mixer
 *
 * @author zhongren.li
 */
public class MixerManager extends BaseConfStreamComponentManager<BaseMixer> {
    //follow页不需要mixerManager打散
    private static final String DONOT_NEED_SHUFFLE = "mx_fetch_followers_content_version_1_0";
    private static final int mixRoundNumber = 1;
    private static final int defaultNumberPerRound = 15;

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.MIXER;
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return null;
    }

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }

    @Override
    @Trace(dispatcher = true)
    public void inProcess(BaseDataCollection dc) {
        //不需要打散
        if (DONOT_NEED_SHUFFLE.equals(dc.req.interfaceName)) {
            //如果mergeList过滤后为空，将保底召回加入返回结果
            if (MXJudgeUtils.isEmpty(dc.mergedList)) {
                dc.data.temp.mixDocumentList.addAll(dc.followGuaranteeList);
            } else {
                dc.data.temp.mixDocumentList.addAll(dc.mergedList);
            }
            return;
        }

        setHighPriorityIds(dc);

        List<BaseDocument> mixDocs = new ArrayList<>();
        for (int i = 0; i < mixRoundNumber; i++) {
            mixForOneRound(dc);
            mixDocs.addAll(dc.data.temp.mixDocumentList);
        }

        dc.data.temp.mixDocumentList.clear();
        dc.data.temp.mixDocumentList.addAll(mixDocs);
    }

    private void mixForOneRound(BaseDataCollection dc) {
        dc.data.temp.mixDocumentList.clear();

        BaseMixer mixerIt;
        for (String mixerName : dc.recommendFlow.mixerList) {
            mixerIt = getComponentInstance(mixerName);
            if (mixerIt != null) {
                mixerIt.process(dc);
            }
        }

        Collections.shuffle(dc.data.temp.mixDocumentList, new JVMRandom());

        for (String guaranteeMixerName : dc.recommendFlow.guaranteeMixerList) {
            mixerIt = getComponentInstance(guaranteeMixerName);
            if (mixerIt != null) {
                mixerIt.process(dc);
            }
        }

        int oneRoundNumber = dc.req.isSetNum() ? dc.req.getNum() : defaultNumberPerRound;
        if (dc.data.temp.mixDocumentList.size() < oneRoundNumber) {
            //1.check offline是不是在guaranteeFirstLevelDocList里。2:添加标记
            int count = 0;
            for (BaseDocument doc : dc.guaranteeSecondLevelDocList) {
                if (count++ > oneRoundNumber) {
                    break;
                }
                doc.setTopHotHistory(true);
            }
            fill(dc, dc.data.temp.mixDocumentList, dc.guaranteeSecondLevelDocList, oneRoundNumber - dc.data.temp.mixDocumentList.size());
        }

        int threshold = (oneRoundNumber) / 3;
        boolean isResultNotEnough = dc.data.temp.mixDocumentList.size() + threshold < oneRoundNumber;
        if (isResultNotEnough) {
            fill(dc, dc.data.temp.mixDocumentList, dc.guaranteeFirstLevelDocList, oneRoundNumber - dc.data.temp.mixDocumentList.size());
        }

        if (dc.data.temp.mixDocumentList.size() < oneRoundNumber) {
            fill(dc, dc.data.temp.mixDocumentList, dc.mergedList, oneRoundNumber - dc.data.temp.mixDocumentList.size());
        }

        Collections.shuffle(dc.data.temp.mixDocumentList, new JVMRandom());
        dc.firstRoundDone = true;
    }

    private void fill(BaseDataCollection dc, List<BaseDocument> target, List<BaseDocument> source, int count) {
        if (MXJudgeUtils.isEmpty(source)) {
            return;
        }
        if (source.size() <= count) {
            // 去重
            target.addAll(new HashSet<>(source));
            source.clear();
        } else {
            int finishCount = 0;
            List<BaseDocument> delDocuments = new ArrayList<>();
            for (BaseDocument doc : source) {
                if (finishCount >= count) {
                    break;
                }

                delDocuments.add(doc);
                if (dc.mixDocumentIdList.contains(doc.id)) {
                    continue;
                }

                target.add(doc);
                dc.mixDocumentIdList.add(doc.id);
                finishCount++;
            }
            source.removeAll(delDocuments);
        }
    }

    /**
     * 把置顶的数据id先提前存到mixDocmentIdList中, 防止后面mix进来的数据重复
     *
     * @param dc
     */
    private void setHighPriorityIds(BaseDataCollection dc) {
        for (BaseDocument doc : dc.highPriorityManualList) {
            doc.setTopHotHistory(true);
            dc.mixDocumentIdList.add(doc.id);
        }
        if (UserProfileDataSource.isPureNewUser(dc)) {
            for (BaseDocument doc : dc.highPriorityVideoForNewUserList) {
                doc.setTopHotHistory(true);
                dc.mixDocumentIdList.add(doc.id);
            }
        }
    }
}