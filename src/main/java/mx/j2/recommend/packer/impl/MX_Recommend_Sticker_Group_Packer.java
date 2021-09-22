package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StickerGroupDocument;
import mx.j2.recommend.thrift.StickerGroup;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/26 下午6:24
 * @description
 */
public class MX_Recommend_Sticker_Group_Packer extends BasePacker {
    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection baseDc) {

        OtherDataCollection dc = (OtherDataCollection)baseDc;
        for (BaseDocument doc : dc.mergedList) {
            if (doc instanceof StickerGroupDocument) {
                StickerGroupDocument sgdoc = (StickerGroupDocument)doc;
                StickerGroup stickerGroup = new StickerGroup();
                stickerGroup.setId(sgdoc.getId());
                stickerGroup.setName(sgdoc.getName());
                stickerGroup.setStatus(sgdoc.getStatus());
                stickerGroup.setOrder(sgdoc.getOrder());
                stickerGroup.setStickerIds(sgdoc.getStickerIds());
                stickerGroup.setOriginalIconUrl(sgdoc.getOriginalIconUrl());
                stickerGroup.setUpdateTime(sgdoc.getUpdateTime());
                stickerGroup.setCreateTime(sgdoc.getCreateTime());
                dc.stickerGroupList.add(stickerGroup);
            }
        }
    }
}
