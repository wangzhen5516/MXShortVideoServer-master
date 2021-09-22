package mx.j2.recommend.data_model.document;

import mx.j2.recommend.util.DefineTool;

/**
 * 音乐
 */
public class MusicTrackDocument extends BaseDocument {

    public MusicTrackDocument() {
        category = DefineTool.CategoryEnum.MUSIC_TRACK;
    }
}
