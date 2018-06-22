# EAFR patch README

This is a patch to implement the replication policy as described in Yuhau Lin and Haiyang Shen,
"[EAFR: An Energy-Efficient Adaptive File Replication System in Data-Intensive Clusters](https://ieeexplore.ieee.org/document/7288402/)" in HDFS.
It also includes code for measuring replication times.
This should be what's necessary to reproduce the experimental results in the paper except for workload generation, but note that this is not
the same code code used to produce the results in that paper.

This code in this patch was written by Nurani Soada, with some adjustments by Charles Reiss.

# Using this

The modifications are contained the hadoop-hdfs package. In the Hadoop distribution this is contained entirely
within `hadoop-hdfs-VERSIONNUMBER.jar`, so building that (in hadoop-hdfs-project/hadoop-hdfs) and replacing
it in a normal binary Hadoop installation should be sufficient.

To use the code in this patch, you need to configure Hadoop:

*  To aid in tracking file accesses, you must configure an audit log to be written on
   the namenode.
   To do this, configure `hadoop-env.sh` to set the `HDFS_AUDIT_LOGGER` environment variable to 
   something like `INFO,RFAAUDIT`, and note the location specified by `log4j.appender.RFAAUDIT.File` 
   (by default the file `hdfs-audit.log` in the HDFS log directory). Then set the configuration
   operation `eafr.access-log` in `hdfs-site.xml` to the location of this file as a URL.
   (e.g. `file:///path/to/hadoop/logs/hdfs-audit.log`.)

*  To enable the customized block placement policy for files marked as "hot" (and not just changes
   in their replication factor), set the configuration option `dfs.block.replicator.classname`
   to `org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyEAFR`.

# Modifications made

1.  Creating org.apache.hadoop.hdfs.server.namenode.AccessLog to process audit logs. This is meant to run on the NameNode,
    and modifications to the main NameNode class start it when the namenode is started. It:

    *  scans an audit log, which it expects to be placed in '/logs/hdfs-audit.log' on the filesystem,
    *  identifies the access frequency of files and chooses to increase or decrease their replication count
    *  marks files as hot or cold for the block placement policies

2.  The new org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyEAFR:
    
    *  uses the EAFR placement algorithm for files that AccessLog indicates are hot. (AccessLog has a static     
       map of hot files, identified by name)
    *  use by setting  dfs.block.replicator.classname  to  org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyEAFR

3.  Changes to org.apache.hadoop.hdfs.server.datanode.BlockReceiver to track exponentially
    weighted moving average of the block transfer times. This is used by BlockPlacementPolicyEAFR

4.  Changes to the heartbeat messages to send the recent EWMA of block transfer times in heartbeat messages.

# Missing Features (need to implement)

*  Periodic rescanning of access log?
*  Unit tests

# Missing Features (optional?)

*  Missing EAFR-type selection for removing replicas
*  Block receiver compenstation for block sizes.
*  Alpha for EWMA configurable?
*  Configurable interval for checking the access log
