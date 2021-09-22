package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.MusicTrackDocument;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;

/**
 * 音乐列表的内容召回
 */
@SuppressWarnings("unused")
public class MusicPlaylistTrackRecall extends BaseMusicItemsRecall {

    @Override
    DefineTool.CategoryEnum getHostCategory() {
        return DefineTool.CategoryEnum.MUSIC_PLAYLIST;
    }

    @Override
    String getItemIdsField() {
        return BaseMagicValueEnum.TRACK_IDS;
    }

    @Override
    BaseDocument newItemDocument() {
        return new MusicTrackDocument();
    }
}
