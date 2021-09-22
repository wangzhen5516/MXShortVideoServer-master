package mx.j2.recommend.scorer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import org.apache.commons.lang.math.RandomUtils;

/**
 * 根据头图长宽是横屏90%的概率减5分
 */
public class ThumbnailScorer extends BaseScorer<BaseDataCollection> {
    private static final int SCORE = -1000;
    private static final double LENGTH_WIDTH_RATIO = 2.5;
    private static final double LENGTH_WIDTH_LINE = 0.9;

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (baseDc.req == null) {
            return true;
        }

        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())) {
            return true;
        }

        return false;
    }

    @Override
    public void score(BaseDataCollection baseDc) {
        for (BaseDocument baseDocument : baseDc.mergedList) {
            if (baseDocument.thumbnailWidth == 0 || baseDocument.thumbnailHeight == 0) {
                if (isNeedMinusScore()) {
                    baseDocument.scoreDocument.minusScore = SCORE;
                }
            }

            if (baseDocument.uploadSign == 1) {
                continue;
            }

            if ((double) baseDocument.thumbnailHeight / (double) baseDocument.thumbnailWidth < LENGTH_WIDTH_LINE) {
                if (isNeedMinusScore()) {
                    baseDocument.scoreDocument.minusScore = SCORE;
                }
            }

            // 高图比例阈值 > 2.5 的过滤
            if ((double) baseDocument.thumbnailHeight / (double) baseDocument.thumbnailWidth > LENGTH_WIDTH_RATIO) {
                if (isNeedMinusScore()) {
                    baseDocument.scoreDocument.minusScore = SCORE;
                }
            }
        }
    }

    private static boolean isNeedMinusScore() {
        int randomInt = RandomUtils.nextInt(101);
        if (randomInt < 10) {
            return false;
        } else {
            return true;
        }
    }
}
