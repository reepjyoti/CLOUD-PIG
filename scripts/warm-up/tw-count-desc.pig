%declare INPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-input/OSN/tw.txt';
%declare OUTPUT_PATH_FR '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/OSN-DESC/twc/';
%declare OUTPUT_PATH_FL '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/OSN-DESC/following/';
%declare OUTPUT_PATH_OL '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/OSN-DESC/outliers/';

dataset = LOAD '$INPUT_PATH' AS (id: long, fr: long);

-- check if user IDs are valid (e.g. not null) and clean the dataset
SPLIT dataset INTO good_dataset IF id is not null and fr is not null, bad_dataset OTHERWISE;

-- organize data such that each node ID is associated to a list of neighbors
nodes = GROUP good_dataset BY id; 

-- foreach node ID generate an output relation consisting of the node ID and the number of "friends"
friends = FOREACH nodes GENERATE group,COUNT(good_dataset) AS followers;
friends = ORDER friends BY group DESC;

-- count the following
nodes2 = GROUP good_dataset BY fr;

followings = FOREACH nodes2 GENERATE group, COUNT(good_dataset);
followings = ORDER followings BY group DESC;

-- find the outliers
outliers = FILTER friends BY followers<3;

STORE friends INTO '$OUTPUT_PATH_FR';
STORE followings INTO '$OUTPUT_PATH_FL';
STORE outliers INTO '$OUTPUT_PATH_OL';
