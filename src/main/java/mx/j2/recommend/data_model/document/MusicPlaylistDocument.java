package mx.j2.recommend.data_model.document;

import mx.j2.recommend.util.DefineTool;

/**
 * 音乐播放列表
 */
public class MusicPlaylistDocument extends BaseDocument {

    public MusicPlaylistDocument() {
        category = DefineTool.CategoryEnum.MUSIC_PLAYLIST;
    }
}
