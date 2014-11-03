hadoop-apriori
==============

Finding frequent pair using Apriori on Hadoop cascading.

Original data of 1.5 GB was prcessed and broken down into important information such as: 
Sessions    Products-visted("|" seperated)

Apriori algorithm is runned to use this inofmration and create L1 and L2 files. 

Runned seuentially first, producing L1, using that information to produce L2, both on Hadoop.


