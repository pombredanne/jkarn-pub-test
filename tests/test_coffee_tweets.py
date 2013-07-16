

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
            self.assertTrue(us_state is not None)
            self.assertTrue(num_coffee_tweets is not None)
            self.assertTrue(num_tweets is not None)
            self.assertTrue(pct_coffee_tweets is not None)

            self.assertTrue(pct_coffee_tweets >= 0.0, pct_coffee_tweets)
            self.assertTrue(pct_coffee_tweets <= 100.0, pct_coffee_tweets)


if __name__ == '__main__':
    main()
