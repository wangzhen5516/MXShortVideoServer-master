package mx.j2.recommend.scorer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

public class LanguageHindiScorer extends BaseScorer<BaseDataCollection> {
	/**
	 *  用来将运营选择的提权
	 */
	private static final long SCORER_ADJUSTER = 10000000;

	@Override
	public boolean skip(BaseDataCollection dc) {
		if (MXJudgeUtils.isEmpty(dc.mergedList)) {
			return true;
		}
		return false;
	}

	/**
	 *
	 * simplescorer
	 *
	 */
	@Override
	@Trace(dispatcher = true)
	public void score(BaseDataCollection dc) {
		for (BaseDocument bdoc : dc.mergedList) {
			if("hi".equals(bdoc.languageId)) {
				bdoc.scoreDocument.languageScore = 5.0f;
			}
		}
	}
}
