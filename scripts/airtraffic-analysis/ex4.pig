SET default_parallel 5;
set job.name 'AIRLINE_EX5_GROUP02';

%declare INPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-input/AIRLINE/2008.csv';
%declare OUTPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/AIRLINE/EX5/';

-- Load raw data
RAW_DATA = LOAD '$INPUT_PATH' USING PigStorage(',') AS 
	(year: int, month: int, day: int, dow: int, 
	dtime: int, sdtime: int, arrtime: int, satime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	etime: int, setime: int, airtime: int, 
	adelay: int, ddelay: int, 
	scode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancel: chararray, cancelcode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

-- A flight is delayed if the delay is greater than 15 minutes. 
-- delay = arrival time - scheduled arrival time
-- Compute the fraction of delayed flights per different time 
-- granularities (hour, day, week, month, year).


-- example: let's focus on a month
-- Foreach month:
-- compute the total number of flights
-- compute delay relation: only those filght with delay > 15 min appear here
-- compute the total number of delayed flights
-- output relation: month, ratio delayed/total


-- project, to get rid of unused fields
A = FOREACH RAW_DATA GENERATE month AS m, carrier, (int)(arrtime-satime) AS delay;

-- group by carrier
B = GROUP A BY carrier;

COUNT_TOTAL = FOREACH B {
	C = FILTER A BY (delay >= 15); -- only keep tuples with a delay >= than 15 minutes
	GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS frac;
}

STORE COUNT_TOTAL INTO '$OUTPUT_PATH';
