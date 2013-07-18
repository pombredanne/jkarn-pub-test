

from pigtest import PigTestCase, main

class SomeTest(PigTestCase):

    PigScript = 'coffee_tweets'

    PigParameters = {
        'INPUT_PATH': '../sample-data/twitter-gardenhose-smaller.json',
        }
    
    def testFinalOutput(self):
        records = list(self.getAlias('ordered_output'))
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
