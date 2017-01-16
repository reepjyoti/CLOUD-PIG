SET default_parallel 5;
set job.name 'AIRLINE_EX1_GROUP02';

%declare INPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-input/AIRLINE/2008.csv';
%declare OUTPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/AIRLINE/EX1/';

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

-- Aggregate in-bound traffic
INBOUND = FOREACH RAW_DATA GENERATE month AS m, dcode AS d;
GROUP_INBOUND = GROUP INBOUND BY (m,d);
COUNT_INBOUND = FOREACH GROUP_INBOUND GENERATE FLATTEN(group), COUNT(INBOUND) AS count;
GROUP_COUNT_INBOUND = GROUP COUNT_INBOUND BY m;
topMonthlyInbound = FOREACH GROUP_COUNT_INBOUND {
    result = TOP(20, 2, COUNT_INBOUND); 
    GENERATE FLATTEN(result);
}
STORE topMonthlyInbound INTO '$OUTPUT_PATH/INBOUND-TOP' USING PigStorage(',');

-- Aggregate out-bound traffic
OUTBOUND = FOREACH RAW_DATA GENERATE month AS m, scode AS s;
GROUP_OUTBOUND = GROUP OUTBOUND BY (m,s);
COUNT_OUTBOUND = FOREACH GROUP_OUTBOUND GENERATE FLATTEN(group), COUNT(OUTBOUND) AS count;
GROUP_COUNT_OUTBOUND = GROUP COUNT_OUTBOUND BY m;
topMonthlyOutbound = FOREACH GROUP_COUNT_OUTBOUND {
    result = TOP(20, 2, COUNT_OUTBOUND); 
    GENERATE FLATTEN(result);
}
STORE topMonthlyOutbound INTO '$OUTPUT_PATH/OUTBOUND-TOP' USING PigStorage(',');

-- Aggregate traffic
UNION_TRAFFIC = UNION COUNT_INBOUND, COUNT_OUTBOUND;
GROUP_UNION_TRAFFIC = GROUP UNION_TRAFFIC BY (m,d);
TOTAL_TRAFFIC = FOREACH GROUP_UNION_TRAFFIC GENERATE FLATTEN(group) AS (m,code), SUM(UNION_TRAFFIC.count) AS total; 
TOTAL_MONTHLY = GROUP TOTAL_TRAFFIC BY m;

topMonthlyTraffic = FOREACH TOTAL_MONTHLY {
    result = TOP(20, 2, TOTAL_TRAFFIC); 
    GENERATE FLATTEN(result) AS (month, iata, traffic);
}


STORE topMonthlyTraffic INTO '$OUTPUT_PATH/MONTHLY-TRAFFIC-TOP/' USING PigStorage(',');

