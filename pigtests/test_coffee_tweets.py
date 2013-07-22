

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
            self.assertIsNotNone(us_state)
            self.assertIsNotNone(num_coffee_tweets)
            self.assertIsNotNone(num_tweets)
            self.assertIsNotNone(pct_coffee_tweets)

            self.assertGreaterEqual(pct_coffee_tweets, 0.0)
            self.assertLessEqual(pct_coffee_tweets, 100.0)


if __name__ == '__main__':
    main()
