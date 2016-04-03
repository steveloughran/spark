---
layout: global
title: Integration with Cloud Infrastructures
---

<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

* This will become a table of contents (this text will be scraped).
{:toc}

## Introduction

Apache Spark

## Important: Cloud object stores are not like filesystems


Object stores are not filesystems: they are not a hierarchical tree of directories and files.

The Hadoop filesystem APIs offer a filesystem API to the object stores, but underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

In particular, the following behaviours are not normally those of a filesysem

### Directory operations may not be atomic nor fast

Directory rename and delete may be performed as a series of operations on the client. Specifically,
`delete(path, recursive=true)` may be implemented as "list the objects, delete them singly or in batches".
`rename(source, dest)` may be implemented as "copy all the objects" followed by the delete operation.


1. They may fail part way through, leaving the status of the filesystem "undefined".
1. The time to delete may be `O(files)`
1. The time to rename may be `O(data)`. If the rename is done on the client, the time to rename
each file will depend upon the bandwidth between client and the filesystem. The further away the client
is, the longer the rename will take.

Because of these behaviours, committing of work by renaming directories is neither efficient nor
reliable. There is a special output committer for Parquet, the `org.apache.spark.sql.execution.datasources.parquet.DirectParquetOutputCommitter`
which bypasses the rename phase.

*Critical* speculative execution does not work against object
stores which do not support atomic directory renames. Your output may get
corrupted

### Data is not written until the output stream's `close()` operation.

Data to be written to the object store is usually buffered to a local file or stored in memory,
until one of: there is enough data to crete a partition in a multi-partitioned upload, or
when the output stream's `close()` operation is done.

- If the process writing the data fails, nothing may have been written.
- Data may be visible in the object store until the entire output stream is complete
- There may not be an entry in the object store for the file (even a 0 byte one) until
that stage.

### An object store's eventual consistency may be visible, especially when updating or deleting data.

Object stores are often *Eventually Consistent*. This can surface, in particular:-

- When listing "a directory"; newly created files may not yet be visible, deleted ones still present.
- After updating an object: opening and reading the object may still return the previous data.
- After deleting an obect: opening it may succeed, returning the data

For many years, Amazon US East S3 lacked create consistency: attempting to open a newly created object
could return a 404 response, which Hadoop maps to a `FileNotFoundException`. This was fixed in August 2015
—see [S3 Consistency Model](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
for the full details.


## Testing Cloud integration

### Example Configuration for testing cloud data


Single test configuration

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///home/hadoop/.ssh/auth-keys.xml"/>


  <property>
    <name>aws.tests.enabled</name>
    <value>true</value>
    <description>Flag to enable AWS tests</description>
  </property>

  <property>
    <name>s3a.test.uri</name>
    <value>s3a://testplan1</value>
    <description>S3A path to a bucket which the test runs are free to write, read and delete
    data.</description>
  </property>
</configuration>
```

This configuration uses XInclude to pull in the secret credentials for the account
from the user's `~/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <value>if.this.key.ever.leaks, reset it in the AWS console</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, without risk.


## Large dataset input tests

Some tests read from large datasets; some simple IO of a multi GB source file,
followed by actual parsing operations of CSV files.

When testing against Amazon s3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used. Specifically

* Large object input.
* CSV parsing: http://landsat-pds.s3.amazonaws.com/scene_list.gz
 that is: s3a://landsat-pds/scene_list.gz

Amazon provide


https://s3.amazonaws.com/datasets.elasticmapreduce/


## Test costs

S3 incurs charges for storage and for IO out of the datacenter where the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon themselves.
