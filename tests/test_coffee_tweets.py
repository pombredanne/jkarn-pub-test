

from testlib import PigTestCase
from unittest import main

class SomeTest(PigTestCase):

    PigScript = 'coffee_tweets'

    PigParameters = {
        'TWEET_LOAD_PATH': '../local_data/tweets/1000_tweets.json',
        'OUTPUT_PATH': '../out/test/',
        }
    
    def testFinalOutput(self):
        records = self.getAlias('ordered_output')
        self.assertTrue(0 < len(records))
        self.assertTrue(51 > len(records))

        for (us_state, num_coffee_tweets, num_tweets, pct_coffee_tweets) in records:
            self.assertIsNotNone(us_state, None)
            self.assertIsNotNone(num_coffee_tweets, None)
            self.assertIsNotNone(num_tweets, None)
            self.assertIsNotNone(pct_coffee_tweets, None)

            self.assertGreaterEqual(pct_coffee_tweets, 0.0, pct_coffee_tweets)
            self.assertLessEqual(pct_coffee_tweets, 100.0, pct_coffee_tweets)


if __name__ == '__main__':
    main()
