## Spark Partitioning Example

### Output:

```
/tmp/spark-tests843519477656602314
├── ds-parquet
│   ├── part-00000-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00001-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00002-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00003-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00004-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00005-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00006-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00007-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00008-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   ├── part-00009-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
│   └── _SUCCESS
└── ds-parquet-partitioned-country
    ├── country=ae
    │   └── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    ├── country=gb
    │   └── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    ├── country=ie
    │   ├── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00001-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   └── part-00002-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    ├── country=us
    │   ├── part-00002-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00003-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00004-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00005-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00006-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00007-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   ├── part-00008-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    │   └── part-00009-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
    └── _SUCCESS

6 directories, 25 files

0       /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/_SUCCESS
120K    /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=ae
1.1M    /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=gb
21M     /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=ie
52M     /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=us
RDD Partitioner: None
21/09/08 13:13:05 WARN TaskSetManager: Stage 2 contains a task of very large size (7531 KiB). The maximum recommended task size is 1000 KiB.
partition: ae -- 100
partition: gb -- 1000
partition: ie -- 20000
partition: us -- 50000
```
