## Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  

## Getting Started

Here we've included some example scripts that explore public data sets. To start using them:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1.  Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:mortardata/mortar-examples.git
        cd mortar-examples
        mortar register mortar-examples

Once you've setup the project, use the `mortar illustrate` command to show data flowing through a given script.  Use `mortar run` to run the script on a Hadoop cluster.

For lots more help and tutorials on running Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.

## Examples

### airline_travel: CSV data from Bureau of Labor Statistics

The [airline_travel](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/airline_travel.pig) pigscript takes data from the [Bureau of Transportation Statistics](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236) and uses it to find out how airlines perform when we normalize for the airports they fly from and to.

### coffee_tweets: JSON data from Twitter

The [coffee_tweets](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/coffee_tweets.pig) pigscript answers the question "Which US state contains the highest concentration of coffee snobs?".  It analyzes and aggregates twitter data from the [twitter-gardenhose](https://github.com/mortardata/twitter-gardenhose), looking for telltale signs of coffee snobbery in tweets.

### common_crawl_trending_topics: Dataset of technology news webpages taken from the Common Crawl

The [common_crawl_trending_topics](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/common_crawl_trending_topics.pig) pigscript finds single-word trending topics by month from a corpus of technology news webpages (techcrunch, gigaom, and allthingsd). It does
this by calculating the frequency of each word in each month, finding the "frequency velocity" from month to month, and selecting the words 
with the highest frequency velocity in each month.

### excite: Search log data from excite! search engine

The [excite](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/excite.pig) pigscript shows an example of loading search engine logs from the excite! search engine and joining them up to a users table.  This is a common pattern for web log analysis.

### millionsong: Million song dataset

Two pigscripts explore the publicly-available [Million Song Dataset](http://labrosa.ee.columbia.edu/millionsong/pages/field-list).

The first, [top_density_songs](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/top_density_songs.pig) finds the songs with the most beats per second in the 1MM song dataset.  Code to ***REALLY FAST*** music!

The second, [hottest_song_of_the_decade](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/hottest_song_of_the_decade.pig) figures out which song is the hottest for each decade of data in the million song dataset.

### nasa_logs: Apache logs from NASA

The [nasa_logs](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/nasa_logs.pig) pigscript is an example of parsing Apache logs to find the most-served resources by date. It takes a sample of two month's worth of logs from 
NASA Kennedy Space Center's web server in 1995 and finds for each date the number of requests served, the number of bytes served, and
the top 10 resources served (images are filtered out since most of the requests are just for icons). It can take a parameter
ORDERING equal to either 'num_requests', to rank resources by the number of requests served, or 'num_bytes', to rank resources
by number of bytes served.

### twitter_sentiment: JSON data from Twitter

The [twitter_sentiment](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/twitter_sentiment.pig) pigscript finds which words are most likely to appear in tweets expressing a "postive sentiment" and which words are most likely to appear in tweets expressing a "negative sentiment". It calculates these likelihoods by looking at the frequency of a word in the corpus of positive/negative tweets diveded by the frequency of that word in the corpus of all processed tweets. The words that cause tweets to be classified as positive/negative (ex. "awesome", "disappointing") in the first place are excluded from the associations, so you can see what caused the sentiments instead of the sentiments themselves. The tweets are taken from the [twitter-gardenhose](https://github.com/mortardata/twitter-gardenhose).

## Advanced examples

### Twitter Pagerank

A separate Mortar project, [twitter-pagerank](https://github.com/mortardata/twitter-pagerank) shows how to embed Pig 
in a Jython controlscript in order to run Pagerank, an algorithm that uses several iteration steps, on a subset of 
the Twitter follower graph. The result is a list of who influential people on Twitter tend to follow. 
There is a [tutorial](http://help.mortardata.com/tutorials/git_projects/working_with_iterative_algorithms) 
on the Mortar help site which walks through twitter-pagerank project step by step.