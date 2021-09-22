package mx.j2.recommend.predictor.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.SageMakerPublisherFeatureDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.predictor.IPredictor;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

/**
 * 预测器基类
 * 
 * @author zhangxuejian
 *
 */
public abstract class BasePredictor<T extends BaseDataCollection> extends BaseStreamComponent<T> implements IPredictor<T> {

	private static final int BASE_PREDICT_NUM = 200;

	/**
	 * 构造函数
	 */
	public BasePredictor() {}

	/**
	 * 子类需要实现的预测器初始化
	 * 
	 */
	public void init() {}

	@Override
	public boolean skip(T data) {
		return false;
	}

	@Override
	public void doWork(T dc) {
		candidateSelectNewExperiment(dc);
		predict(dc);
	}

	void candidateSelect(T dc) {
		List<BaseDocument> documents = new ArrayList<>();
		Set<String> idSet = new HashSet<>();

		mix(documents, dc.userFollowPublishList, idSet);
		mix(documents, dc.userFollowVPublishList, idSet);
		mix(documents, dc.userPrePubDocList, idSet);
		mix(documents, dc.userProfileOfflineRecommendList, idSet);

		for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMap.entrySet()) {
			mix(documents, entry.getValue(), idSet);
		}
		documents.sort((o1, o2) -> Double.compare(o2.heatScore2, o1.heatScore2));

		for (int i = 0; i < BASE_PREDICT_NUM && i < documents.size(); i++) {
			dc.predictDocumentList.add(documents.get(i));
			dc.userProfileCount.add(documents.get(i).getRecallName());
		}
	}

	void candidateSelectNewExperiment(T dc) {
		List<BaseDocument> documents = new ArrayList<>();
		Set<String> idSet = new HashSet<>();

		mix(documents, dc.userFollowPublishList, idSet);
		mix(documents, dc.userFollowVPublishList, idSet);
		mix(documents, dc.userPrePubDocList, idSet);
		mix(documents, dc.userProfileOfflineRecommendList, idSet);

		for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMap.entrySet()) {
			mix(documents, entry.getValue(), idSet);
		}
		documents.sort((o1, o2) -> Double.compare(o2.heatScore2, o1.heatScore2));

		for (int i = 0; i < documents.size(); i++) {
			dc.predictDocumentList.add(documents.get(i));
			dc.userProfileCount.add(documents.get(i).getRecallName());
		}
	}

	/**
	 * 选择候选人，目前按照base混入系数*10来添加到predictDocumentList，每部分选择方式按照heat_score2排序
	 * @param dc
	 */

	private void candidateSelectExperiment(T dc) {
		List<BaseDocument> documents = new ArrayList<>();
		Set<String> idSet = new HashSet<>();

		compareAndMix(dc.userFollowPublishList, documents, idSet, 5);
		compareAndMix(dc.userFollowVPublishList, documents, idSet, 10);
		compareAndMix(dc.userPrePubDocList, documents, idSet, dc.ratioForRealTimeMixer * 10);
		compareAndMix(dc.userProfileOfflineRecommendList, documents, idSet, 50);
		for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMap.entrySet()) {
			compareAndMix(entry.getValue(),documents, idSet,10);
		}
		for (int i = 0; i < BASE_PREDICT_NUM && i < documents.size(); i++) {
			dc.predictDocumentList.add(documents.get(i));
			dc.userProfileCount.add(documents.get(i).getRecallName());
		}
	}

	private void compareAndMix(List<BaseDocument> source, List<BaseDocument> target,Set<String>idSet, int mixNum){
		if(MXJudgeUtils.isEmpty(source)){
			return;
		}
		List<BaseDocument> copy = new ArrayList<>(source);
		copy.sort((o1, o2) -> Double.compare(o2.heatScore2, o1.heatScore2));
		mixExperiment(target, copy, idSet, mixNum);
	}

	private void mixExperiment(List<BaseDocument> to, List<BaseDocument> from, Set<String> idSet, int mixNum){
		if (MXJudgeUtils.isEmpty(from)||mixNum == 0) {
			return;
		}
		int limit = Math.min(mixNum,from.size());
		for(int i = 0; i< limit;i++){
			BaseDocument doc = from.get(i);
			if(idSet.contains(doc.id)){
				continue;
			}
			doc.duration = doc.getDuration() == 0 ? doc.getInnerDuration() : doc.getDuration();
			to.add(doc);
			idSet.add(doc.id);
		}
	}

	private void mix(List<BaseDocument> to, List<BaseDocument> from, Set<String> idSet) {
		if (MXJudgeUtils.isEmpty(from)) {
			return;
		}

		from.forEach(doc -> {
			if (idSet.contains(doc.id)) {
				return;
			}
			doc.duration = doc.getDuration() == 0 ? doc.getInnerDuration() : doc.getDuration();
			to.add(doc);
			idSet.add(doc.id);
		});
	}

	String getFeatureString(T dc){
		UserProfileDataSource userProfileDataSource = MXDataSource.profile();
		Set<String> pubIds = new HashSet<>();
		for(BaseDocument document : dc.predictDocumentList){
			if(MXStringUtils.isNotEmpty(document.publisher_id)){
				pubIds.add(document.publisher_id);
			}
		}
		Map<String, SageMakerPublisherFeatureDocument> pubFeatureDocMap = MXDataSource.redis().getPubFeatureInfoFromRedis(pubIds);
		String userProfileResult = userProfileDataSource.getUserProfileByUuId(dc.client.user.uuId);
		String userStr = userProfileDataSource.getUserProfile(userProfileResult);

		List<String> featureList = userProfileDataSource.getPrediction(dc.client.user.uuId, dc.predictDocumentList, userProfileResult);
		StringBuilder builder = new StringBuilder();

		SageMakerPublisherFeatureDocument defaultSagePubDoc = new SageMakerPublisherFeatureDocument();
		for (int i = 0; i < dc.predictDocumentList.size(); i++) {
			BaseDocument document = dc.predictDocumentList.get(i);
			StringBuilder sb = new StringBuilder();
			sb.append(userStr);
			sb.append(',');
			sb.append(document.sageMakerVideoFeatureDocument);
			sb.append(',');
			SageMakerPublisherFeatureDocument sagePubDoc = pubFeatureDocMap.getOrDefault(document.publisher_id, defaultSagePubDoc);
			sb.append(sagePubDoc);
			sb.append(',');
			sb.append(featureList.get(i));
			sb.append('\n');
			builder.append(sb);
			dc.featureString = sb.toString();
		}
		return builder.toString();
	}
}
