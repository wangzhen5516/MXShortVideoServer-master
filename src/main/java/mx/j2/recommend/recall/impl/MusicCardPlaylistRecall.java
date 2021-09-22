package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.MusicPlaylistDocument;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;

/**
 * 音乐播放列表卡片的内容召回
 */
@SuppressWarnings("unused")
public class MusicCardPlaylistRecall extends BaseMusicItemsRecall {

    @Override
    DefineTool.CategoryEnum getHostCategory() {
        return DefineTool.CategoryEnum.CARDNEW;
    }

    @Override
    String getItemIdsField() {
        return BaseMagicValueEnum.PLAYLIST_IDS;
    }

    @Override
    BaseDocument newItemDocument() {
        return new MusicPlaylistDocument();
    }
}
