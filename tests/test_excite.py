from testlib import PigTestCase
from unittest import main

class TestExcite(PigTestCase):

    PigScript = 'excite'

    def testCleanSearchesFiltersNoData(self):
        self.stubAlias('searches',[
            ['steve', '12345', ''],
            ['', '12346', "why aren't I using google"],
            ['mark', '123457', 'how to unit test apache pig'],
            ])
        self.assertAliasEquals('clean_searches', [
            ('mark', '123457', 'how to unit test apache pig'),
            ])

    def testAgeBucketing(self):
        self.stubAlias('users', [
            ['mark', 31],
            ['steve', 23]
            ])
        self.assertAliasEquals('users_age_buckets', [
            ('mark', '30 - 39'),
            ('steve', '20 - 29'),
            ])

    def testAgeBucketMetrics(self):
        self.stubAlias('clean_searches', [
            ('mark', 123457, 'how to unit test apache pig'),
            ('steve', 123456, 'how is babey formed'),
            ('mark', 123457, 'italian food in little italy'),
            ('steve', 123457, 'italian food in big italy'),
            ('steve', 123457, 'italian food in big ben'),
            ])
        self.stubAlias('users_age_buckets', [
            ('mark', '30 - 39'),
            ('steve', '20 - 29'),
            ])
        self.assertAliasEquals('age_buckets', [
            ('20 - 29', 3, 4),
            ('30 - 39', 2, 4.18181818182),
            ])


if __name__ == '__main__':
    main()
