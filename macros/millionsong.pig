/**
 * millionsong: Macros for accessing the million song dataset.
 *              See the data spec for million song dataset at http://bit.ly/vOBKPe
 */

/**
 * Load up all million songs.
 */
DEFINE ALL_SONGS()
RETURNS songs {
    -- Load up the million song dataset from S3 (see data spec at: http://bit.ly/vOBKPe)
    $songs = LOAD 's3n://tbmmsd/*.tsv.*' USING PigStorage('\t') AS (
         track_id:chararray, analysis_sample_rate:chararray, artist_7digitalid:chararray,
         artist_familiarity:chararray, artist_hotness:double, artist_id:chararray, artist_latitude:chararray, 
         artist_location:chararray, artist_longitude:chararray, artist_mbid:chararray, artist_mbtags:chararray, 
         artist_mbtags_count:chararray, artist_name:chararray, artist_playmeid:chararray, artist_terms:chararray, 
         artist_terms_freq:chararray, artist_terms_weight:chararray, audio_md5:chararray, bars_confidence:chararray, 
         bars_start:chararray, beats_confidence:chararray, beats_start:chararray, danceability:double, 
         duration:float, end_of_fade_in:chararray, energy:chararray, key:chararray, key_confidence:chararray, 
         loudness:chararray, mode:chararray, mode_confidence:chararray, release:chararray, 
         release_7digitalid:chararray, sections_confidence:chararray, sections_start:chararray, 
         segments_confidence:chararray, segments_loudness_max:chararray, segments_loudness_max_time:chararray, 
         segments_loudness_max_start:chararray, segments_pitches:chararray, segments_start:chararray, 
         segments_timbre:chararray, similar_artists:chararray, song_hotness:double, song_id:chararray, 
         start_of_fade_out:chararray, tatums_confidence:chararray, tatums_start:chararray, tempo:double, 
         time_signature:chararray, time_signature_confidence:chararray, title:chararray, track_7digitalid:chararray, 
         year:int );
};

/**
 * Load up one songs file only (useful for running on smaller clusters).
 */
DEFINE ONE_SONGS_FILE()
RETURNS songs {
    -- Load up only one file from the million song dataset
    $songs = LOAD 's3n://tbmmsd/A.tsv.a' USING PigStorage('\t') AS (
         track_id:chararray, analysis_sample_rate:chararray, artist_7digitalid:chararray,
         artist_familiarity:chararray, artist_hotness:double, artist_id:chararray, artist_latitude:chararray, 
         artist_location:chararray, artist_longitude:chararray, artist_mbid:chararray, artist_mbtags:chararray, 
         artist_mbtags_count:chararray, artist_name:chararray, artist_playmeid:chararray, artist_terms:chararray, 
         artist_terms_freq:chararray, artist_terms_weight:chararray, audio_md5:chararray, bars_confidence:chararray, 
         bars_start:chararray, beats_confidence:chararray, beats_start:chararray, danceability:double, 
         duration:float, end_of_fade_in:chararray, energy:chararray, key:chararray, key_confidence:chararray, 
         loudness:chararray, mode:chararray, mode_confidence:chararray, release:chararray, 
         release_7digitalid:chararray, sections_confidence:chararray, sections_start:chararray, 
         segments_confidence:chararray, segments_loudness_max:chararray, segments_loudness_max_time:chararray, 
         segments_loudness_max_start:chararray, segments_pitches:chararray, segments_start:chararray, 
         segments_timbre:chararray, similar_artists:chararray, song_hotness:double, song_id:chararray, 
         start_of_fade_out:chararray, tatums_confidence:chararray, tatums_start:chararray, tempo:double, 
         time_signature:chararray, time_signature_confidence:chararray, title:chararray, track_7digitalid:chararray, 
         year:int );
};


/**
 * Load up a very small subset of local songs data for testing.
 */
DEFINE LOCAL_SONGS_FILE_SMALL()
RETURNS songs {
    -- Load up only one file from the million song dataset
    $songs = LOAD '../data/songs_small_local.txt.gz' USING PigStorage('\t') AS (
         track_id:chararray, analysis_sample_rate:chararray, artist_7digitalid:chararray,
         artist_familiarity:chararray, artist_hotness:double, artist_id:chararray, artist_latitude:chararray, 
         artist_location:chararray, artist_longitude:chararray, artist_mbid:chararray, artist_mbtags:chararray, 
         artist_mbtags_count:chararray, artist_name:chararray, artist_playmeid:chararray, artist_terms:chararray, 
         artist_terms_freq:chararray, artist_terms_weight:chararray, audio_md5:chararray, bars_confidence:chararray, 
         bars_start:chararray, beats_confidence:chararray, beats_start:chararray, danceability:double, 
         duration:float, end_of_fade_in:chararray, energy:chararray, key:chararray, key_confidence:chararray, 
         loudness:chararray, mode:chararray, mode_confidence:chararray, release:chararray, 
         release_7digitalid:chararray, sections_confidence:chararray, sections_start:chararray, 
         segments_confidence:chararray, segments_loudness_max:chararray, segments_loudness_max_time:chararray, 
         segments_loudness_max_start:chararray, segments_pitches:chararray, segments_start:chararray, 
         segments_timbre:chararray, similar_artists:chararray, song_hotness:double, song_id:chararray, 
         start_of_fade_out:chararray, tatums_confidence:chararray, tatums_start:chararray, tempo:double, 
         time_signature:chararray, time_signature_confidence:chararray, title:chararray, track_7digitalid:chararray, 
         year:int );
};
