# Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  

# Getting Started

Here we've included some example scripts that explore public data sets. To start using them:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1.  Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:mortardata/mortar-examples.git
        cd mortar-examples
        mortar register mortar-examples

For more help and tutorials on running Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.

# Examples

## airline_travel

The [airline_travel](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/airline_travel.pig) pigscript takes data from the [Bureau of Transportation Statistics] (http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236) and uses it to find out how airlines perform when we normalize for the airports they fly from and to.

## coffee_tweets

The [coffee_tweets](https://github.com/mortardata/mortar-examples/blob/master/pigscripts/coffee_tweets.pig) pigscript answers the question "Which US state contains the highest concentration of coffee snobs?".  It analyzes and aggregates twitter data from the [twitter-gardenhose](https://github.com/mortardata/twitter-gardenhose), looking for telltale signs of coffee snobbery in tweets.
