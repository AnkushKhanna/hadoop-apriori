hadoop-apriori
==============

Finding frequent pair using Apriori on Hadoop cascading.

Original data of 1.5 GB was prcessed and broken down into important information such as:

Sessions    Products-visted("|" seperated)

Apriori algorithm is runned on this inofmration and create L1 and L2 files. 

Runned seuentially first producing L1, using L1 via DistributedCache to produce L2. All proces run on hadoop.


