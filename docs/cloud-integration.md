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

Apache Spark can use cloud object stores as a source or destination of data. It does so
through filesystem connectors implemented in apache Hadoop. Provided the relevant libraries
are on the classpath, a file can be referenced simply via its URL

```scala
sparkContext.textFile("s3a://landsat-pds/scene_list.gz").count()
```

Similarly, an RDD can be saved to an object store via `saveAsTextFile()`


```scala
val numbers = sparkContext.parallelize(1 to 1000)
numbers.textFile("swift://publicdataset/updates")
numbers.textFile("wasb://landsat@example.blob.core.windows.net/updates")
```

While they can be used as the source and destination of data, they cannot be
used as a direct replacement for a cluster-wide filesystem, such as HDFS.
This is important to know, as the fact they are easy to work with can be misleading.

## Cloud object stores are not filesystems

Object stores are not filesystems: they are not a hierarchical tree of directories and files.

The Hadoop filesystem APIs offer a filesystem API to the object stores, but underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

Many behaviors expected of a filesystem are emulated in the object store APIs, but only
imperfectly.

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
reliable. There is a special output committer for Parquet,
the `org.apache.spark.sql.execution.datasources.parquet.DirectParquetOutputCommitter`
which bypasses the rename phase.


*Critical* speculative execution does not work against object
stores which do not support atomic directory renames. Your output may get
corrupted.

*Warning* even non-speculative execution is at risk of leaving the output of a job in an inconsistent
state if a "Direct" output committer is used and executors fail.


### Data may not be written until the output stream's `close()` operation.

Data to be written to the object store is usually buffered to a local file or stored in memory,
until one of: there is enough data to create a partition in a multi-partitioned upload (where enabled),
or when the output stream's `close()` operation is done.

- If the process writing the data fails, nothing may have been written.
- Data may be visible in the object store until the entire output stream is complete
- There may not be an entry in the object store for the file (even a 0 byte one) until
that stage.

### An object store may display eventual consistency

Object stores are often *Eventually Consistent*. This can surface, in particular:-

- When listing "a directory"; newly created files may not yet be visible, deleted ones still present.
- After updating an object: opening and reading the object may still return the previous data.
- After deleting an obect: opening it may succeed, returning the data.
- While reading an object, if it is updated or deleted during the process.

For many years, Amazon US East S3 lacked create consistency: attempting to open a newly created object
could return a 404 response, which Hadoop maps to a `FileNotFoundException`. This was fixed in August 2015
—see [S3 Consistency Model](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
for the full details.

### Read operations may be significantly slower than normal filesystem operations.

Object stores usually implement their APIs as HTTP operations; clients make HTTP(S) requests
and block for responses. Each of these calls can be expensive. For maximum performance

1. Try to list filesystem paths in bulk.
1. Know that `FileSystem.getFileStatus()` is expensive: cache the results rather than repeat
the call (or wrapper methods such as `FileSystem.exists(), isDirectory() or isFile()`).
1. Try to forward seek through a file, rather than backwards.
1. Avoid renaming files: This is slow and, if it fails, may fail leave the destination in a mess.
1. Use the local filesystem as the destination of output which you intend to reload in follow-on work.
Retain the object store as the final destination of persistent output, not as a replacement for
HDFS.


## Object stores and their library dependencies

The different object stores supported by Spark depend on specific Java libraries.


### Amazon S3 with s3a://

The "S3A" filesystem is a connector with Amazon S3, initially implemented in Hadoop 2.6, and
considered ready for production use in Hadoop 2.7.

The implementation is `hadoop-aws`, which is included in the `spark-assembly` JAR when spark
is built against Hadoop 2.6 or later.

Dependencies: `amazon-aws-sdk` JAR (Hadoop 2.6 and 2.7); `amazon-s3-sdk` and `amazon-core-sdk`
in Hadoop 2.8. *Warning*: The Amazon JARs have proven very brittle —the version of the Amazon
libraries must match that which the Hadoop binaries were built against.

### Amazon S3 with s3n://

The "S3N" filesystem connector is a long-standing connector shipping with all versions of Hadoop 2.
It uses the `jets3t` library to talk to HDFS; this must be on the classpath.

### Microsoft Azure with wasb://

The `wasb` filesystem connector is implemented in `hadoop-aws` and built into the `spark-assembly`
JAR. It needs the `azure-storage` JAR on the classpath.

It is only present if Spark was built against Hadoop 2.7 or later.

### Openstack Swift


## Testing Cloud integration

The `spark-cloud` module contains tests which can run against the object stores. These verify
functionality integration and performance.

### Example Configuration for testing cloud data


This is a configuration enabling the S3A and Azure test, referencing the secret credentials
kept in another file.

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

  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://MYCONTAINER@TESTACCOUNT.blob.core.windows.net</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/hadoop/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>SECRET_AWS_KEY</value>
  </property>
  <property>
    <name>fs.azure.account.key.TESTACCOUNT.blob.core.windows.net</name>
    <value>SECRET_AZURE_KEY</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.


## Large dataset input tests

Some tests read from large datasets; some simple IO of a multi GB source file,
followed by actual parsing operations of CSV files.

### Amazon S3 test datasets

When testing against Amazon S3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used. Specifically

* Large object input.
* CSV parsing: `http://landsat-pds.s3.amazonaws.com/scene_list.gz`, which can be referenced
as an S3A file as `s3a://landsat-pds/scene_list.gz`


## Running a single test case

Each cloud test takes time; it is convenient to be able to work on a single test case at a time
when implementing or debugging a test.

Every test has a *key* name which SHOULD BE unique within the specific test class; it MAY BE
even across the entire module —though this does not hold for subclassed tests.

As an example, here is a test create and save a test data to an object store using the
Hadoop filesystem API via the RDD function `saveAsNewAPIHadoopFile()`

```scala

  ctest("NewHadoopAPI", "New Hadoop API",
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, sc.hadoopConfiguration)
  }
```

The test is defined with a key, `NewHadoopAPI`, a name for the XML/HTML reports,
`New Hadoop API`, and a description for logging (and perhaps future XML reports).


This method can be exclusively executed by passing it to maven in the property `test.method.keys`

```

# running all (possibly subclassed) instantations of this method in scalatest suites.
mvn test -Phadoop-2.7 -Dcloud.test.configuration.file=cloud.xml -Dtest.method.keys=NewHadoopAPI

# running the test purely in the S3A suites
mvn test -Phadoop-2.7 -DwildcardSuites=org.apache.spark.cloud.s3.S3aIOSuite -Dcloud.test.configuration.file=cloud.xml

# running two named tests across all filesystems
mvn test -Phadoop-2.7 -Dcloud.test.configuration.file=cloud.xml -Dtest.method.keys=NewHadoopAPI,CSVgz

# test run against Hadoop "branch-2"
mvt -Phadoop-2.7  -Dcloud.test.configuration.file=../cloud.xml -Dhadoop.version=2.9.0-SNAPSHOT
```

The combination of scalatest naming via the `wildcardSuites` property with the test-case specific
key allows developers to easily focus on the failure or performance issues of a single test case
within the module

## Best practices for adding a new test

1. Use the `ctest()` declaration of a test case conditional on the suite being enabled.
1. Give it a uniqe key using upper-and-lower-case letters and numerals only.
1. Give it a name useful in test reports/bug reports
1. Give it a meaningful description.


## Best practices for adding a new test suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Support parallel operation.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.


## Test costs

S3 incurs charges for storage and for IO out of the datacenter where the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon themselves.
