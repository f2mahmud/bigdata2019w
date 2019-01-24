Answers to questions from Assignment 1
======================================

Ans 1 :   
-------
Pairs implementation: 
	The main idea here is to create a pair between each 2 different words in a line. The pairs from each line has to be unique and then
	eventually added up, to get the coocccurence count. Side data to count the total number of lines in which a word occured is gathered
	through another job. This side job would returen a String, int pair which would be used to calculate the PMI. There were 30 MapReduce jobs.
	The input records are number of lines i.e. 122458. The intermediete value is a key value pair of PairOfStrings and int. The final value would
	be a Key value pair of pair of strings and double & int to store the PMI and cocurrency as requred.
Stripes Implementation:
	While the input records are the same, the data is initially arranged in a way where a word is they key and a String-int map is the value.
	The values in the map are the number of times the string occcured in the same line with the word. The intermediete and final data type 
	are the same. Here there are also 30 MapReduce jobs.

Ans 2 : 
-------
In the linux environment:
	Time taken for pairs implementation: 26.17 seconds
	Time taken for stripes implementation: 15.15 seconds

Ans 3 :
-------
 In the linux environment:
	Time taken for pairs implementation: 28.08
	Time taken for stripes implementation:15.173

Ans 4 : 
-------
34639 distinct pairs

Ans 5 : 
-------
Highest PMI: (maine, anjou)	(.6331423021951013
Lowest PMI: (thy, you)	-1.5303967668481646

This means that for maine and anjou, the chance of one being writtein in a line where the other one already exists i very high.
For thy and you, howevr, the probaility of one coming with another in the same line are quite low.

Ans 6 : 
-------
With tears:
 shed	2.1117900768762365
 salt   2.0528122169168985
 eyes	1.1651669643071034

Ans 7 : 
-------


Ans 8 : 
-------