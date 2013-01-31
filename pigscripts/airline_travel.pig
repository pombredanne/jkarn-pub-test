/**
 * airline_travel: Find the best and the worst air travel providers
 *
 * Required parameters:
 *
 * -param OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/millionsong)
 */

%default INPUT_PATH 's3://mortar-example-data/airline-data/634173101_T_ONTIME.csv'
%default OUTPUT_PATH 's3n://hawk-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/mortar-examples'

REGISTER '../udfs/python/airline_travel.py' USING streaming_python AS airline_travel;

--Load airline data
raw_data = LOAD 's3://mortar-example-data/airline-data/634173101_T_ONTIME.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage('SKIP_HEADER')
AS (year:int, month: int, unique_carrier:chararray, origin_airport_id:chararray, dest_airport_id:chararray,
dep_delay:int, dep_delay_new: int, arr_delay:int, arr_delay_new:int, cancelled:int);

--Create a penalty on cancelled flights by considering them as having an arrival delay of 5 hours
apply_penalty = FOREACH raw_data GENERATE unique_carrier, origin_airport_id, dest_airport_id, airline_travel.null_to_zero(dep_delay_new) AS dep_delay, airline_travel.create_penalty(arr_delay_new) AS arr_delay;

--Get the average departure delay for each airport
group_by_departure_airport =  GROUP apply_penalty BY (origin_airport_id);
avg_delay_departure_airport = FOREACH group_by_departure_airport GENERATE group, AVG(apply_penalty.dep_delay) AS avg_departure_delay;
--Get the average arrival delay for each airport
group_by_arrival_airport =  GROUP apply_penalty BY (dest_airport_id);
avg_delay_arrival_airport = FOREACH group_by_departure_airport GENERATE group, AVG(apply_penalty.arr_delay) AS avg_arrival_delay;

--Get the worst airports so we can look at them
avg_delay_departure_airport_ordered =  ORDER avg_delay_departure_airport BY avg_departure_delay DESC;
avg_delay_arrival_airport_ordered =  ORDER avg_delay_arrival_airport BY avg_arrival_delay DESC;
worst_departures = LIMIT avg_delay_departure_airport_ordered 20;
worst_arrivals = LIMIT avg_delay_arrival_airport_ordered 20;

--join our flight data to the average arrival delay for the relevant arrival and departure airports
join_arrival_delay = JOIN apply_penalty by origin_airport_id, avg_delay_departure_airport by group;
join_both_delay = JOIN join_arrival_delay by dest_airport_id, avg_delay_arrival_airport by group;

--Calculate a normalized delay score for each flight by taking (arrival_delay - avg_arrival_delay) - avg_departure_delay
--This penalizes an airline for having a larger-than-average arrival delay, and attempts to normalize for flights that leave from delayed versus less delayed airports
normalized_data_final = FOREACH join_both_delay GENERATE unique_carrier, ((arr_delay - avg_arrival_delay) - avg_departure_delay) as normalized_arrival_delay;

--Get the normalized badness for each airline
normalized_data_by_airline = GROUP normalized_data_final BY (unique_carrier);
avg_normalized_delay_airline =  FOREACH normalized_data_by_airline GENERATE group, AVG(normalized_data_final.normalized_arrival_delay) AS normalized_arrival_delay;

sorted_avg_normalized_delay_airline = ORDER avg_normalized_delay_airline by normalized_arrival_delay;

rmf $OUTPUT_PATH/worst_arrival_airports;
rmf $OUTPUT_PATH/worst_departure_airports;
rmf $OUTPUT_PATH/ranked_airlines;
STORE worst_arrivals INTO '$OUTPUT_PATH/worst_arrival_airports' USING PigStorage('\t');
STORE worst_departures INTO '$OUTPUT_PATH/worst_departure_airports' USING PigStorage('\t');
STORE sorted_avg_normalized_delay_airline INTO '$OUTPUT_PATH/ranked_airlines' USING PigStorage('\t');




