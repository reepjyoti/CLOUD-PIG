# Hadoop Pig Laboratory - Solution
This is the solution for the [Hadoop Pig Laboratory at Eurecom](https://github.com/michiard/CLOUDS-LAB/tree/master/labs/pig-lab)
##Outline
- Warm up exercises
    + word count
    + Online social network
- Use-case 1 - Working with Network Traffic data
- Use-case 2 - Working with an Airline dataset

## Warm up exercises
### Exercise 1. Word Count
#####Script: scripts/warm-up/word-count.pig

#####Q1: Compare between Pig and Hadoop, including their pros and cons

| Pig                                 | Hadoop MR                           |
| ----------------------------------- | ----------------------------------- |
| - Short, concise, quickly written   | - Long, requires effort to write    |
| - More reusability, easier to maintain| - Code is difficult to reuse & maintain|
| - Optimizations are done automatically | - Optimizations are extensively done by developers|
| - Good performance, has been increasing overtime | - Better performance (sometimes)|
| - Supports limited set of operations & UDF | - Developers have full control|

#####Q2: What does a GROUP BY command do? In which phase of MapReduce is GROUP BY performed in this exercise and in general?
- GROUP BY: group rows by key
- The map phase assigns keys for grouping, the reduce phase do the grouping

#####Q3: What does a FOREACH command do? In which phase of MapReduce is FOREACH performed in this exercise and in general?
- FOREACH: iterates row-by-row through the whole dataset to performs operations.
- possible in both in map only phase, or reduce phase

#####Q4: Explain very briefly how Pig works (i.e. the process of Pig turning a Pig Latin script into runnable MapReduce job(s))
- Pig Latin scripts are compiled into MapReduce jobs, and executed using Hadoop
- Lazy execution model is applied: no processing is carried out when constructing the logical plans.
- Steps:
    + Step 1: Build the Logical Plan (the DAG graph): 
	   * The Pig interpreter parses the commands (Parser)
	   * Then it verifies validity of input files and bags (variables)
	   * Pig builds a logical plan for every bag
    + Step 2: Build the Physical Plan:
	   * Compiler converts logical commands to MapReduce jobs
	   * With optimizations
    + Step 3: runs jobs on cluster

###Exercise 2. Working with Online Social Networks data
###Exercise 2.1. Counting the number of "friends" per Twitter user
#####Script: scripts/warm-up/ex2

#####Q1: Is the output sorted? Why?
- Yes, the output is sorted
- Because the key is declared in the schema as long numbers. The job involves the reduce phase, and it will sort the key using default sorter.

#####Q2: Can we impose an output order, ascending or descending? How?
- Yes
- Use ORDER ... BY ... [column_name ASC|DESC] statement

#####Q3: Related to job performance, what kinds of optimization does Pig provide in this exercise? Are they useful? Can we disable them? Should we?
- Multiquery execution
- Yes, it minimizes the I/O: try to read the input once, in single job.
- Yes, we can disable them by:
	+ `pig -x local -M script.pig`
	+ `pig -x local -no_multiquery script.pig`
- No, we shouldn't

#####Q4: What should we do when the input has some noise? for example: some lines in the dataset only contain USER_ID but the FOLLOWER_ID is unavailable or null
- We should partition the input data to good and bad records.
- `SPLIT records INTO good_records IF ... is not null, bad _records IF ... is NULL;`

### Exercise 2.2. Find the number of two-hop paths in the Twitter network
#####Script: scripts/warm-up/ex2
#####Q1: What is the size of the input data? In your opinion, is it considered Big Data? Why? How long does it take for the job to complete in your case? What are the cause of a poor performance of a job?
- Size: 11.7 Mb, 904215 records
- Big? no
- How long: Pig script completed in 29 minutes, 36 seconds and 405 milliseconds (1776405 ms)
- Causes: 
	+ Cluster load
	+ I/O (multiple jobs)
	+ Join operation requires shuffling data alot

#####Q2: Try to set the parallelism with different number of reducers.What can you say about the load balancing between reducers?
- Some reducers (4) have to work as 2-3 times as the other reducers (not good)

#####Q3: Explain briefly how Pig JOIN works in your opinion.
- First, it may depend on the JOIN algorithm that Pig chose (eg: Replicated Join, Skewed Join, Merge Join, ...)
- For regular join:
    * Map: Emmits (key, record, [table_flag]) tuples in each dataset. (key $1 & $2 for db1 & db2 respectively)
    * Reduce: Forms tuples (from different dataset) having the same key

#####Q4: Have you verified your results? Does your result contain duplicate tuples? Do you have loops (tuples that points from one user to the same user)? What operations do you use to remove duplicates?
- Yes, verified.
- No duplicates
- No loops
- Use the DISTINCT operation

#####Q5: How many MapReduce jobs does your Pig script generate? Explain why
- 4 MapReduce jobs
- Explain: 
    + Load dataset A
    + Load dataset B (because Pig doesn't allow self-join!)
    + Join operation will result in 1 job. We have 1
    + Distinct operation will result in 1 job. We have 2
- Verify:
    + `EXPLAIN result;` #each MRNode is one MR job
    + `pig -x local -e 'explain -script /home/reepjyoti/IdeaProjects/CloudLabEurecom-Pig/scripts/ex2/tw-join.pig' > ~/explain.txt`
`grep MapReduce ~/explain.txt`

##Use-case 1. Working with Network Traffic data
[TSTAT Trace Analysis with Pig](https://github.com/michiard/CLOUDS-LAB/blob/master/labs/pig-lab/tstat-analysis/README.md)

###Exercise 1. A "Network" Word Count
####1A. Count the number of TCP connections per each client IP
- Script: scripts/tstat/tcp_count_1a.pig

####1B. Count the number of TCP connection per each IP
- Script: scripts/tstat/tcp_count_1b.pig

#####Q1: How is this exercise different from Ex. 1?
- Count #connections for each IP (client & server) rather than just clientIP
- We have to do 2 group by: client ip, server ip. Then we have group by again on the union of previous output.

#####Q2: How would you write it in Java?
MapReduce:
- 1 jobs:
    + map: for each record, emmit 2 tuples: <client_ip, 1>, <server_ip, 1>
    + possibly with combiner
    + reduce: for each key, count(values)

#####Q3:Elaborate on the differences between Ex.1 and Ex.1/b
- Difference in the key to groupby
- 1b: multiple query execution, pipe jobs

####1C.Cube & Rollup
- Script: scripts/tstat/tcp_count_1c.pig

###Exercise 2
- Script: scripts/tstat/ex2.pig
- `filter $113 == "google.it"`

###Exercise 3
- Script: scripts/tstat/ex3.pig

###Exercise 4
- Script: scripts/tstat/ex4.pig

#####Q1: Is this job map-only? Why? Why not? 
- No
- This job requires `group by` & `sort` which require shuffling data

#####Q2: Where did you apply the TOP function?
- used sort & limit

#####Q3: Can you explain how does the TOP function work? 
- sort desc then limit

#####Q4: The TOP function was introduced in PIG v.0.8. How, in your opinion, and based on your understanding of PIG, was the query answered before the TOP command was available? Do you think that it was less efficient than the current implementation?
- in progress

###Exercise 5
- Script: scripts/tstat/ex5.pig

#####Q1: How many jobs were generated? 
- 6 jobs

#####Q2: Describe how the join is executed
- Replicated Join?

###Exercise 6
- Script: scripts/tstat/ex6.pig

#####Q1: Did you obtain any "strange" values?
- Yes, if the mss_c is null or empty, we will obtain blank output

#####Q2: What did you learn from this exercise? Is your data generally "clean"?
- We should split data into good & bad records first.

###Exercise 7
- Typo: 1460 -> 14600
- Script: scripts/tstat/ex7.pig

#####Q1: How many MR jobs were generated by PIG?
- 2 jobs

#####Q2: How many reducers were launched per each job? (did you use the PARALLEL keyword?)
- ?
- No, I set default parallel value: `SET default_parallel 5;`

###Exercise 8
- Script: scripts/tstat/ex8.pig

#####Q1: How many reducers were launched? Which degree of parallelism did you choose? Why?
- ?

###Exercise 9
- Script: scripts/tstat/ex9.pig

#####Q1: Using the result of this exercise and the previous one, what can you say about the distribution of server ports?
- From the output of exercise 9, 72.3% of flows connect to server at port 80
- From the output of exercise 8, 79.84% of data transmitted to server at port 80

#####Q2:
- i. Using the MR web interface (or, alternatively, the log files generated by Hadoop), find the number of keys processed by each reducer. Do you expect to have a sensible difference in the number of processed distinct keys?
- ii. Is the reducers load unbalanced?
- iii. How would you avoid an eventual skew?

## Use-case 2. Working with an Airline dataset
[Airline Data Analysis with Pig](https://github.com/michiard/CLOUDS-LAB/blob/master/labs/pig-lab/airtraffic-analysis/README.md)

###Query 1: Top 20 airports by total volume of flights
- Script: scripts/airtraffic-analysis/ex1.pig

###Query 2: Carrier Popularity
- Script: scripts/airtraffic-analysis/ex2.pig

###Query 3: Proportion of Flights Delayed
- Script: scripts/airtraffic-analysis/ex3.pig

###Query 4: Carrier Delays
- Script: scripts/airtraffic-analysis/ex4.pig

###Query 5: Busy Routes
- Script: scripts/airtraffic-analysis/ex5.pig

###Optional data analysis tasks
- When is the best time of day/day of week/time of year to fly to minimize delays?
- Do older planes suffer more delays?
- How does the number of people flying between different locations change over time?
- How well does weather predict plane delays?
- Can you detect cascading failures as delays in one airport create delays in others? Are there critical links in the system?