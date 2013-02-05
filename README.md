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

For more help and tutorials on running Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.

## Examples

### airline_travel: CSV data from Bureau of Labor Statistics

The [airline_travel](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/airline_travel.pig) pigscript takes data from the [Bureau of Transportation Statistics] (http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236) and uses it to find out how airlines perform when we normalize for the airports they fly from and to.

### coffee_tweets: JSON data from Twitter

The [coffee_tweets](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/coffee_tweets.pig) pigscript answers the question "Which US state contains the highest concentration of coffee snobs?".  It analyzes and aggregates twitter data from the [twitter-gardenhose](https://github.com/mortardata/twitter-gardenhose), looking for telltale signs of coffee snobbery in tweets.

### excite: Search log data from excite! search engine

The [excite](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/excite.pig) pigscript shows an example of loading search engine logs from the excite! search engine and joining them up to a users table.  This is a common pattern for web log analysis.

### nasa_logs: Apache logs from NASA

The [nasa_logs](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/nasa_logs.pig) pigscript is an example of parsing Apache logs to find the most-served resources by date. It takes a sample of two month's worth of logs from 
NASA Kennedy Space Center's web server in 1995 and finds for each date the number of requests served, the number of bytes served, and
the top 10 resources served (images are filtered out since most of the requests are just for icons). It can take a parameter
ORDERING equal to either 'num_requests', to rank resources by the number of requests served, or 'num_bytes', to rank resources
by number of bytes served.
