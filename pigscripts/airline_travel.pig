/**
 * airline_travel: Find the best and the worst air travel providers.
 *
 * There are plenty of sites (e.g http://www.flightstats.com/) that give statistics on airlines, both in aggregate and broken down by route.  
 * But suppose I live in a big transit hub like NYC, and want to pick one airline to be the one I accrue frequent flyer miles on.  
 * I want the airline with the fewest delays, but I don't want that to be PuddleJumpers Inc, just because they only fly 
 * to small airports that get two flights a day.
 */

-- Set our input and output path as pig parameters
%default INPUT_PATH 's3n://mortar-example-data/airline-data'
%default OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/airline-travel'

-- Register the python functions we use in the pigscript
REGISTER '../udfs/python/airline_travel.py' USING streaming_python AS airline_travel;

-- Load CSV airline data from the Bureau of Transportation Statistics
-- (see http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)
raw_data = LOAD '$INPUT_PATH' 
          USING org.apache.pig.piggybank.storage.CSVExcelStorage('SKIP_HEADER')
             AS (year:int, month: int, unique_carrier:chararray, 
                 origin_airport_id:chararray, dest_airport_id:chararray,
                 dep_delay:int, dep_delay_new: int, arr_delay:int, arr_delay_new:int, cancelled:int);

-- Create a penalty on cancelled flights by considering them as having an arrival delay of 5 hours
--  (if we don't do this, then they would just count as nothing, and that seems just seems wrong).
apply_penalty = FOREACH raw_data 
               GENERATE unique_carrier, origin_airport_id, dest_airport_id, 
                        airline_travel.null_to_zero(dep_delay_new) AS dep_delay, 
                        airline_travel.create_penalty(arr_delay_new) AS arr_delay;

-- Get the average departure delay for each airport
-- It's possible that an airport may be great at letting flights land 
-- but be terrible about having them take off, or vice versa, so we need
-- to treat departure and arrival airports as separate entities.
group_by_departure_airport =  GROUP apply_penalty BY (origin_airport_id);
avg_delay_departure_airport = FOREACH group_by_departure_airport 
                             GENERATE group, AVG(apply_penalty.dep_delay) AS avg_departure_delay;

-- Get the average arrival delay for each airport
group_by_arrival_airport =  GROUP apply_penalty BY (dest_airport_id);
avg_delay_arrival_airport = FOREACH group_by_departure_airport 
                           GENERATE group, AVG(apply_penalty.arr_delay) AS avg_arrival_delay;

-- Get the worst airports so we can look at them
avg_delay_departure_airport_ordered =  ORDER avg_delay_departure_airport BY avg_departure_delay DESC;
avg_delay_arrival_airport_ordered =  ORDER avg_delay_arrival_airport BY avg_arrival_delay DESC;
worst_departures = LIMIT avg_delay_departure_airport_ordered 20;
worst_arrivals = LIMIT avg_delay_arrival_airport_ordered 20;

-- Join our flight data to the average arrival delay for the relevant arrival and departure airports
join_arrival_delay = JOIN apply_penalty BY origin_airport_id, avg_delay_departure_airport BY group;
join_both_delay = JOIN join_arrival_delay BY dest_airport_id, avg_delay_arrival_airport BY group;

-- In order to get a "normalized" delay, we take the arrival delay for a flight (how late it actually got in), 
-- subtract off the average arrival delay for that airport, and then also subtract off the average departure delay 
-- for the departure airport, to get `normalized_data_final`.
-- This penalizes an airline for having a larger-than-average arrival delay, and attempts to 
-- normalize for flights that leave from delayed versus less delayed airports
-- This should make comparison between airlines that fly to different airports "fair."
normalized_data_final = FOREACH join_both_delay 
                       GENERATE unique_carrier, ((arr_delay - avg_arrival_delay) - avg_departure_delay) as normalized_arrival_delay;

--Get the normalized badness for each airline
normalized_data_by_airline = GROUP normalized_data_final BY (unique_carrier);
avg_normalized_delay_airline =  FOREACH normalized_data_by_airline 
                               GENERATE group, AVG(normalized_data_final.normalized_arrival_delay) AS normalized_arrival_delay;

-- Now let's take the average delay for each airline and sort it.  
-- Interestingly, most of the numbers are negative, which means that planes 
-- tend to make up time in the air-the departure delay is generally bigger than the arrival delay.  
-- The sorted list tells us which airlines would perform best if all airports were created equal.
sorted_avg_normalized_delay_airline = ORDER avg_normalized_delay_airline BY normalized_arrival_delay;

-- Remove any existing data
rmf $OUTPUT_PATH/worst_arrival_airports;
rmf $OUTPUT_PATH/worst_departure_airports;
rmf $OUTPUT_PATH/ranked_airlines;
STORE worst_arrivals INTO '$OUTPUT_PATH/worst_arrival_airports' USING PigStorage('\t');
STORE worst_departures INTO '$OUTPUT_PATH/worst_departure_airports' USING PigStorage('\t');
STORE sorted_avg_normalized_delay_airline INTO '$OUTPUT_PATH/ranked_airlines' USING PigStorage('\t');
