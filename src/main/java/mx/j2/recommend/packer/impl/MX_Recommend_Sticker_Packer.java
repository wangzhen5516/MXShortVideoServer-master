package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StickerDocument;
import mx.j2.recommend.thrift.Sticker;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/29 下午2:21
 * @description
 */
public class MX_Recommend_Sticker_Packer extends BasePacker {
    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection baseDc) {

        OtherDataCollection dc = (OtherDataCollection)baseDc;
        for (BaseDocument doc : dc.mergedList) {
            if (doc instanceof StickerDocument) {
                StickerDocument sdoc = (StickerDocument)doc;
                Sticker sticker = new Sticker();
                sticker.setId(sdoc.getId());
                sticker.setStickerName(sdoc.getStickerName());
                sticker.setStatus(sdoc.getStatus());
                sticker.setStickerType(sdoc.getStickerType());
                sticker.setStickerGroup(sdoc.getStickerGroup());
                sticker.setOriginalPackageUrl(sdoc.getOriginalPackageUrl());
                sticker.setStickerThumbnailUrl(sdoc.getStickerThumbnailUrl());
                sticker.setCountries(sdoc.getCountries());
                sticker.setUpdateTime(sdoc.getUpdateTime());
                sticker.setCreateTime(sdoc.getCreateTime());
                sticker.setOriginalStickerUrl(sdoc.getOriginalStickerUrl());
                dc.stickerList.add(sticker);
            }
        }
    }
}
