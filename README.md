hadoop-apriori
==============

Finding frequent pair using Apriori on Hadoop cascading.

Original data of 1.5 GB was processed and broken down into important information such as:

Sessions    Products-visited("|" separated)

Apriori algorithm is runned on information above and create L1 and L2 files. 

Runned sequentially producing L1 and using L1 via DistributedCache to produce L2. All process run on hadoop.
