%declare INPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-input/OSN/twitter-big-sample.txt';
%declare OUTPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/OSN-JOIN/';

SET default_parallel 20;
-- TODO: load the input dataset, located in ./local-input/OSN/tw.txt
datasetA = LOAD '$INPUT_PATH' AS (id: long, fr: long);
datasetB = LOAD '$INPUT_PATH' AS (id: long, fr: long);

SPLIT datasetA INTO good_datasetA IF id is not null and fr is not null, bad_datasetA OTHERWISE;
SPLIT datasetB INTO good_datasetB IF id is not null and fr is not null, bad_datasetB OTHERWISE;


-- TODO: compute all the two-hop paths 
twohop = JOIN good_datasetA BY fr, good_datasetB BY id;

-- TODO: project the twohop relation such that in output you display only the start and end nodes of the two hop path
p_result = FOREACH twohop GENERATE $0, $3;

-- TODO: make sure you avoid loops (e.g., if user 12 and 13 follow eachother) 
d_result = DISTINCT p_result;
result = FILTER d_result BY $0 != $1;

STORE result INTO '$OUTPUT_PATH';
