# Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  You create your project using the Mortar Development Framework, deploy code using the Git revision control system, and Mortar does the rest.  Here we've included some example scripts to help you get started using Mortar.

# Getting Started

For help getting started with Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.

# Examples

## Which Airline is the Best?
There are plenty of [sites] (http://www.flightstats.com/) that give statistics on airlines, both in aggregate and broken down by route.  But suppose I live in a big transit hub like NYC, and want to pick one airline to be the one I accrue frequent flyer miles on.  I want the airline with the fewest delays, but I don't want that to be PuddleJumpers Inc, just because they only fly to small airports that get two flights a day.

The `airline-travel` project takes uses data from the [Bureau of Transportation Statistics] (http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236) and uses it to find out how airlines perform when we normalize for the airports they fly from and to.

First we apply a penalty to cancelled flights (if we don't do this, then they would just count as nothing, and that seems just seems wrong).  This is stored in `apply_penalty`.

Then, we calculate the average delay for each departure airport&mdash;`avg_delay_departure_airport`, and each arrival airport&mdash;`avg_delay_arrival_airport`.  It's possible that an airport may be great at letting flights land but be terrible about having them take off, or vice versa, so we need to treat departure and arrival airports as separate entities.  We'll store this data out, because it might be fun to look at later.

In order to get a "normalized" delay, we take the arrival delay for a flight (how late it actually got in), subtract off the average arrival delay for that airport, and then also subtract off the average departure delay for the departure airport, to get `normalized_data_final`.  This should make comparison between airlines that fly to different airports "fair."

Now let's take the average delay for each airline and sort it&mdash;`sorted_avg_normalized_delay_airline`.  Interestingly, most of the numbers are negative, which means that planes tend to make up time in the air--the departure delay is generally bigger than the arrival delay.  The sorted list tells us which airlines would perform best if all airports were created equal.

## What Webpages was NASA Serving in 1995?
This project is an example of parsing Apache logs to find the most-served resources by date. It takes a sample of two month's worth of logs from 
NASA Kennedy Space Center's web server in 1995 and finds for each date the number of requests served, the number of bytes served, and
the top 10 resources served. It can take a parameter ORDERING equal to either 'num_requests', to rank resources by the number of requests served,
or 'num_bytes', to rank resources by number of bytes served.

The output is not particularly interesting (though it was a bit surprising that NASA was serving hundreds of megabytes a day of video files
all the way back in 1995), but the project demonstrates some tricky Pig concepts such as how to use group by a heirarchy of keys
(in this case, first by URI and then by date) and how to use a nested FOREACH statement.
