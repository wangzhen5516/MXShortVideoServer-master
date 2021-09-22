package mx.j2.recommend.data_model.data_collection;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.CardListItemDocument;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Card;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 数据集合
 *
 * @author zhongren.li
 */
@NotThreadSafe
public class FetchTabsDataCollection extends BaseDataCollection {
    public List<Card> cardList;

    /**
     * 构造函数
     */
    public FetchTabsDataCollection() {
        super();
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        cardList = new ArrayList<>();
    }


    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void clean() {
        this.baseClean();
        cardList = null;
        cardList = new ArrayList<>();
    }

    /**
     * 创建rpc的结果response
     */
    @Trace(dispatcher = true)
    public void generateResponse() {
        if (MXJudgeUtils.isNotEmpty(cardList)) {
            data.response.cardList = new ArrayList<>();
            for (Card card : cardList) {
                data.response.cardList.add(card.deepCopy());
                if (req.num <= data.response.getCardListSize()) {
                    if (cardList.indexOf(card) < cardList.size() - 1) {
                        data.response.nextToken = cardList.get(cardList.indexOf(card) + 1).cardId;
                    }
                    break;
                }
            }
        }
        if (null != req.getLogId()) {
            data.response.setLogId(req.getLogId());
        } else if (isDebugModeOpen) {
            if (null == debug.deletedRecordMap) {
                return;
            }
            JSONObject json = new JSONObject(16, true);
            List<Map.Entry<String, Integer>> list = new ArrayList<>(debug.deletedRecordMap.entrySet());
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            for (Map.Entry<String, Integer> entry : list) {
                json.put(entry.getKey(), entry.getValue());
            }
            data.response.setLogId(json.toString());
        }
    }


    public void generateResponseNew() {
        if (MXJudgeUtils.isNotEmpty(cardList)) {
            data.response.cardList = new ArrayList<>();
            int counter = 0;
            for (Card card : cardList) {
                data.response.cardList.add(card.deepCopy());
                //加入计数器
                counter++;
                if (req.num <= data.response.getCardListSize()) {
                    //可能有问题
                    CardListItemDocument doc = (CardListItemDocument) this.mergedList.get(counter - 1);
                    data.response.nextToken = String.valueOf(doc.getOrder());
                    break;
                }
            }
        }
        if (null != req.getLogId()) {
            data.response.setLogId(req.getLogId());
        } else if (isDebugModeOpen) {
            if (null == debug.deletedRecordMap) {
                return;
            }
            JSONObject json = new JSONObject(16, true);
            List<Map.Entry<String, Integer>> list = new ArrayList<>(debug.deletedRecordMap.entrySet());
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            for (Map.Entry<String, Integer> entry : list) {
                json.put(entry.getKey(), entry.getValue());
            }
            data.response.setLogId(json.toString());
        }
    }


    /**
     * 将请求中的信息置到dc中
     */
    @Trace(dispatcher = true)
    public boolean loadRequest(Request req) {
        if (null == req) {
            this.req = BaseDataCollection.EMPTY_REQUEST;
            return false;
        }
        this.req = req;

        if (MXJudgeUtils.isNotEmpty(req.isDebugModeOpen)) {
            try {
                isDebugModeOpen = Boolean.parseBoolean(req.isDebugModeOpen);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        RecommendFlow flow = null;
        /*debug模式选择绑定flow*/
        if (isDebugModeOpen) {
            flow = MXDataSource.flow()
                    .getRecoFlowByUuId(this, req.getInterfaceName(), req.getUserInfo().getUuid());
        }
        /*此处没开debug模式或者debug模式没拿到走保底*/
        if (flow == null) {
            flow = MXDataSource.flow()
                    .getRecommendFlowByInterfaceName(this, req.getInterfaceName(), req.getUserInfo().getUuid());
        }
        this.recommendFlow = flow;

        if (null == this.recommendFlow) {
            return false;
        }
        String requestType = DefineTool.FlowInterface.findFlowInterfaceByName(req.getInterfaceName(), DefineTool.FlowInterface.DEFAULT).getType();
        if (null == requestType) {
            return false;
        }

        if (MXJudgeUtils.isNotEmpty(req.userInfo.uuid)) {
            this.client.user.uuId = req.userInfo.uuid;
        } else {
            this.client.user.uuId = "nullUuId";
        }

        if (MXJudgeUtils.isNotEmpty(req.userInfo.userId)) {
            this.client.user.userId = req.userInfo.userId;
        } else {
            this.client.user.userId = "nullUserId";
        }

        if (req.num == 0) {
            req.num = Conf.getDefaultRequestNumber();
        }

        return true;
    }
}
