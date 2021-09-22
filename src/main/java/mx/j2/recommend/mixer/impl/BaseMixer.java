package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.base.BaseConfigurableMixer;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.lang.math.JVMRandom;

import java.util.*;
import java.util.function.Predicate;

import static mx.j2.recommend.util.BaseMagicValueEnum.NewUserWatchNumber;
import static mx.j2.recommend.util.BaseMagicValueEnum.YoungUserWatchNumber;

/**
 * @param <T> DC 类型
 * @author ：zhongrenli
 * @author : zhendong.wang
 * @version 2.0
 * @date ：Created in 3:42 下午 2020/8/13
 * @date : 2021/2/26
 * @description Mixer 都是可配置的，所以改为继承自 BaseConfigurableMixer
 */
public abstract class BaseMixer<T extends BaseDataCollection> extends BaseConfigurableMixer<T> {

    private Random random = new JVMRandom();

    @Override
    public void doWork(T dc) {
        mix(dc);
    }

    public BaseDocument getOneFromSource(List<BaseDocument> source) {
        if (MXJudgeUtils.isEmpty(source)) {
            return null;
        }
        BaseDocument bd = source.get(0);
        if (bd.isFollowed) {
            return source.remove(0);
        } else {
            int i = random.nextInt(source.size());
            return source.remove(i);
        }
    }

    public void moveToList(T dc, List<BaseDocument> toAdd, double ratio, List<BaseDocument> source) {
        if (MXJudgeUtils.isNotEmpty(source)) {
            double ret = random.nextDouble();
            if (1 >= ratio) {
                if (ret <= ratio) {
                    toAdd.add(getOneFromSource(source));
                }
            } else {
                int number = (int) ratio;
                for (int i = number; i > 0; i--) {
                    if (0 >= source.size()) {
                        break;
                    }
                    toAdd.add(getOneFromSource(source));
                }
                //计算去掉整数后的部分  2.5 -> 2 和 0.5的概率
                ratio -= number;
                ret = random.nextDouble();
                if (ret <= ratio && MXJudgeUtils.isNotEmpty(source)) {
                    toAdd.add(getOneFromSource(source));
                }
            }
        }
    }

    public void moveToListWithTag(T dc, List<BaseDocument> toAdd, double ratio, List<BaseDocument> source, PoolConf pc) {
        try {
            if (MXJudgeUtils.isNotEmpty(source)) {
                double ret = random.nextDouble();
                if (1 >= ratio) {
                    if (ret <= ratio) {
                        toAdd.addAll(getNDocsFromSource(dc, source, pc, 1));
                    }
                } else {
                    int number = (int) ratio;
                    toAdd.addAll(getNDocsFromSource(dc, source, pc, number));
                    //计算去掉整数后的部分  2.5 -> 2 和 0.5的概率
                    ratio -= number;
                    ret = random.nextDouble();
                    if (ret <= ratio && MXJudgeUtils.isNotEmpty(source)) {
                        toAdd.addAll(getNDocsFromSource(dc, source, pc, 1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private List<BaseDocument> getNDocsFromSource(T dc, List<BaseDocument> source, PoolConf pc, int n) {
        if (MXJudgeUtils.isEmpty(pc.nicheTags) && MXJudgeUtils.isEmpty(pc.generalTags)) {
            return getNoTagsDocs(source, pc, n);
        }

        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getLongTermTags(dc);
        if (MXJudgeUtils.isEmpty(tags)) {
            return getNoTagsDocs(source, pc, n);
        }

        List<String> tagString = new ArrayList<>();
        tags.forEach(tag -> tagString.add(tag.name));

        List<BaseDocument> docs = new ArrayList<>();
        int mixerCount = n;
        if (MXJudgeUtils.isNotEmpty(pc.nicheTags)) {
            List<String> temp = new ArrayList<>(tagString);
            temp.retainAll(pc.nicheTags.toJavaList(String.class));

            if (MXJudgeUtils.isNotEmpty(temp)) {
                List<BaseDocument> r = getDocs(source, temp, n);
                docs.addAll(r);
                mixerCount -= r.size();
                if (MXJudgeUtils.isNotEmpty(docs) && mixerCount <= 0) {
                    return docs;
                }
            }
        }

        if (MXJudgeUtils.isNotEmpty(pc.generalTags) && mixerCount > 0) {
            List<String> temp = new ArrayList<>(tagString);
            temp.retainAll(pc.generalTags.toJavaList(String.class));

            if (MXJudgeUtils.isNotEmpty(temp)) {
                List<BaseDocument> r = getDocs(source, temp, mixerCount);
                docs.addAll(r);
                mixerCount -= r.size();
                if (MXJudgeUtils.isNotEmpty(docs) && mixerCount <= 0) {
                    return docs;
                }
            }
        }

        return getNoTagsDocs(source, pc, mixerCount);
    }

    private List<BaseDocument> getDocs(List<BaseDocument> source, List<String> tags, int n) {
        List<BaseDocument> docs = new ArrayList<>();
        for (BaseDocument doc : source) {
            if (docs.size() >= n) {
                return docs;
            }

            List<String> tagString = getTagStrings(doc);
            if (MXJudgeUtils.isEmpty(tagString)) {
                continue;
            }

            List<String> s = new ArrayList<>(tagString);
            s.retainAll(tags);
            if (MXJudgeUtils.isNotEmpty(s)) {
                doc.setRetainTags(s);
                docs.add(doc);
            }
        }
        return docs;
    }

    private List<BaseDocument> getNoTagsDocs(List<BaseDocument> source, PoolConf pc, int n) {
        List<BaseDocument> docs = new ArrayList<>();
        for (int i = n; i > 0; i--) {
            if (0 >= source.size()) {
                break;
            }
            while (!source.isEmpty()) {
                BaseDocument doc = getOneFromSource(source);
                List<String> tags = getTagStrings(doc);
                if (MXJudgeUtils.isEmpty(pc.nicheTags)) {
                    docs.add(doc);
                    break;
                }
                tags.retainAll(pc.nicheTags.toJavaList(String.class));
                if (MXJudgeUtils.isNotEmpty(tags)) {
                    continue;
                }
                docs.add(doc);
                break;
            }
        }
        return docs;
    }

    private List<String> getTagStrings(BaseDocument doc) {
        List<String> tagStrings = new ArrayList<>();
        Set<String> tagSet = new HashSet<>();

        if (MXJudgeUtils.isNotEmpty(doc.getMlTags())) {
            tagSet.addAll(doc.getMlTags());
        }

        if (MXJudgeUtils.isNotEmpty(doc.getPrimaryTags())) {
            tagSet.addAll(doc.getPrimaryTags().toJavaList(String.class));
        }

        if (MXJudgeUtils.isNotEmpty(tagSet)) {
            tagStrings.addAll(tagSet);
        }
        return tagStrings;
    }

    // 类似moveToList，将随机从sourceList取改为按顺序从sourceList头部取
    public void moveToListInOrder(T dc, List<BaseDocument> toAdd, double ratio, List<BaseDocument> source) {
        if (MXJudgeUtils.isNotEmpty(source)) {
            double ret = random.nextDouble();
            if (1 >= ratio) {
                if (ret <= ratio) {
                    toAdd.add(source.get(0));
                    source.remove(0);
                }
            } else {
                int number = (int) ratio;
                for (int i = number; i > 0; i--) {
                    if (0 >= source.size()) {
                        break;
                    }
                    toAdd.add(source.get(0));
                    source.remove(0);
                }
                //计算去掉整数后的部分  2.5 -> 2 和 0.5的概率
                ratio -= number;
                ret = random.nextDouble();
                if (ret <= ratio && MXJudgeUtils.isNotEmpty(source)) {
                    toAdd.add(source.remove(0));
                }
            }
        }
    }

    /**
     * 条件混入
     */
    void moveToListOnCondition(List<BaseDocument> toAdd, double ratio, List<BaseDocument> source, Predicate<BaseDocument> predicate) {
        int num = (int) ratio;
        ratio -= num;

        // 小数部分命中，多加一个
        if (ratio > 0 && ratio >= random.nextDouble()) {
            num++;
        }

        Iterator<BaseDocument> iterator = source.iterator();
        BaseDocument documentIt;

        while (num > 0 && iterator.hasNext()) {
            documentIt = iterator.next();

            if (predicate.test(documentIt)) {
                toAdd.add(documentIt);
                iterator.remove();
                num--;
            }
        }
    }

    public void moveToListInOrderWithDifferentPublisher(T dc, List<BaseDocument> toAdd, double ratio, List<BaseDocument> source) {
        if (MXJudgeUtils.isNotEmpty(source)) {
            double ret = random.nextDouble();
            if (1 >= ratio) {
                if (ret <= ratio) {
                    toAdd.add(source.get(0));
                    source.remove(0);
                }
            } else {
                Set<String> set = new HashSet<>();
                int number = (int) ratio;
                for (int i = number; i > 0; ) {
                    if (0 >= source.size()) {
                        break;
                    }
                    BaseDocument doc = source.get(0);
                    if (set.contains(doc.publisher_id)) {
                        source.remove(0);
                        continue;
                    }
                    toAdd.add(doc);
                    set.add(doc.publisher_id);
                    source.remove(0);
                    i--;
                }
                //计算去掉整数后的部分  2.5 -> 2 和 0.5的概率
                ratio -= number;
                ret = random.nextDouble();
                if (ret <= ratio && MXJudgeUtils.isNotEmpty(source)) {
                    toAdd.add(source.remove(0));
                }
            }
        }
    }

    public void addOneDocToMixDocument(T dc, BaseDocument doc) {
        if (dc.mixDocumentIdList.contains(doc.id)) {
            return;
        }
        dc.data.temp.mixDocumentList.add(doc);
        dc.mixDocumentIdList.add(doc.id);
    }

    public void addOneDocToMixDocumentHead(T dc, BaseDocument doc) {
        if (dc.mixDocumentIdList.contains(doc.id)) {
            return;
        }
        dc.data.temp.mixDocumentList.add(0, doc);
        dc.mixDocumentIdList.add(doc.id);
    }

    public boolean addDocToMixDocument(T dc, BaseDocument doc) {
        if (dc.mixDocumentIdList.contains(doc.id)) {
            return false;
        }

        dc.data.temp.mixDocumentList.add(doc);
        dc.mixDocumentIdList.add(doc.id);
        return true;
    }

    public void addDocsToMixDocument(T dc, List<BaseDocument> docs) {
        if (MXJudgeUtils.isEmpty(docs)) {
            return;
        }
        for (BaseDocument doc : docs) {
            addOneDocToMixDocument(dc, doc);
        }
    }

    public void addDocsToMixDocumentHead(T dc, List<BaseDocument> docs) {
        if (MXJudgeUtils.isEmpty(docs)) {
            return;
        }
        for (BaseDocument doc : docs) {
            addOneDocToMixDocumentHead(dc, doc);
        }
    }

    public void cutMixDocumentFromHead(T dc, int cutSize) {
        if (dc.data.temp.mixDocumentList.size() < cutSize) {
            return;
        }
        List<BaseDocument> toDelet = new ArrayList<>(dc.data.temp.mixDocumentList.subList(cutSize, dc.data.temp.mixDocumentList.size()));

        for (BaseDocument doc : toDelet) {
            dc.data.temp.mixDocumentList.remove(doc);
            dc.mixDocumentIdList.remove(doc.id);
        }
    }

    public boolean isPureNewUser(T dc) {
        return UserProfileDataSource.isPureNewUser(dc);
    }

    public boolean isYoungUser(T dc) {
        return YoungUserWatchNumber > dc.userHistorySize && NewUserWatchNumber <= dc.userHistorySize;
    }

    public void getGender(T baseDc) {
        UserProfileDataSource userProfileDataSource = MXDataSource.profile();
        String userProfile = userProfileDataSource.getUserProfileByUuId(baseDc.client.user.uuId);
        String gender = userProfileDataSource.getUserGenderInfo(userProfile, baseDc.client.user.userId);
        if (MXStringUtils.isNotEmpty(gender)) {
            baseDc.client.user.profile.gender = gender;
        }
    }

    boolean resultIsEnough(BaseDataCollection dc) {
        return dc.data.temp.mixDocumentList.size() >= dc.req.num;
    }

    public static void main(String[] args) {
        double ratio = 1.8;
        int o = (int) ratio;
        System.out.println(ratio - o);
    }
}