%declare INPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-input/WORD_COUNT/sample.txt';
%declare OUTPUT_PATH '/home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/local-output/WORD_COUNT/';

A = LOAD '$INPUT_PATH';
B = FOREACH A GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
C = FILTER B BY word MATCHES '\\w+';
D = GROUP C BY word;
E = FOREACH D GENERATE group, COUNT(C);
STORE E INTO '$OUTPUT_PATH';
