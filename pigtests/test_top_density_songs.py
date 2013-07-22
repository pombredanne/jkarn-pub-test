from pigtest import PigTestCase, main

class TestExcite(PigTestCase):

    PigScript = 'top_density_songs'


    def generateRecord(self, fields):
        return (
            fields.get('track_id'), fields.get('analysis_sample_rate'),
            fields.get('artist_7digitalid'), fields.get('artist_familiarity'),
            fields.get('artist_hotness'), fields.get('artist_id'),
            fields.get('artist_latitude'),  fields.get('artist_location'),
            fields.get('artist_longitude'), fields.get('artist_mbid'), fields.get('artist_mbtags'), 
            fields.get('artist_mbtags_count'), fields.get('artist_name'), fields.get('artist_playmeid'),
            fields.get('artist_terms'),  fields.get('artist_terms_freq'), fields.get('artist_terms_weight'),
            fields.get('audio_md5'), fields.get('bars_confidence'), fields.get('bars_start'),
            fields.get('beats_confidence'), fields.get('beats_start'), fields.get('danceability'), 
            fields.get('duration'), fields.get('end_of_fade_in'), fields.get('energy'),
            fields.get('key'), fields.get('key_confidence'), fields.get('loudness'),
            fields.get('mode'), fields.get('mode_confidence'), fields.get('release'), 
            fields.get('release_7digitalid'), fields.get('sections_confidence'), fields.get('sections_start'), 
            fields.get('segments_confidence'), fields.get('segments_loudness_max'),
            fields.get('segments_loudness_max_time'), fields.get('segments_loudness_max_start'),
            fields.get('segments_pitches'), fields.get('segments_start'), 
            fields.get('segments_timbre'), fields.get('similar_artists'), fields.get('song_hotness'),
            fields.get('song_id'), fields.get('start_of_fade_out'), fields.get('tatums_confidence'),
            fields.get('tatums_start'), fields.get('tempo'), fields.get('time_signature'),
            fields.get('time_signature_confidence'), fields.get('title'), fields.get('track_7digitalid'), 
            fields.get('year')
            )




    def testFilterDuration(self):
        self.mockAlias('songs', [
            self.generateRecord({'track_id': '12345', 'duration':1.2}),
            self.generateRecord({'track_id': '12346', 'duration':0}),
            self.generateRecord({'track_id': '12347', 'duration':0}),
            self.generateRecord({'track_id': '12348', 'duration':100}),
            ])
        self.assertAliasEquals('filtered_songs', [
            self.generateRecord({'track_id': '12345', 'duration':1.2}),
            self.generateRecord({'track_id': '12348', 'duration':100}),
            ])

    
