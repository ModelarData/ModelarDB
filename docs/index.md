# User Manual

[toc]

This user manual for ModelarDB is meant to give a quick overview of how the system is installed, queried and what the returned results look like.



## Install

ModelarDB can be obtained as either a classical `Jar` file or as a `Docker` image.

### Jar File

Download ModelarDB Zip file from:

https://github.com/ModelarData/ModelarDB/releases

and extract it to a folder of your chosing.

It is a pre-requisit that you have Java 8 or above installed (we have testet it with Java 8 and Java 11).



**Running the Jar File**

To run ModelarDB as a jar file do

```sh
java -jar ModelarDB-0.1.0.jar <config-file>
```

When you run ModelarDB you must either specify a config file on the command-line or have one present in your home directory at `$HOME/.modelardb.conf`. For more info about the config file see the [config](#config) section below.

The Zip file also contains the [REDD](http://redd.csail.mit.edu/) dataset. In order to run ModelarDB with this dataset already ingested run it using the `redd-query.conf` config file like so

```sh
java -jar ModelarDB-0.1.0.jar redd-query.conf
```



### Docker

Alternatively you can also get ModelarDB as a Docker image

```sh
docker pull ghcr.io/modelardata/modelardb:latest
```

This image also comes with the [REDD](http://redd.csail.mit.edu/) dataset included and can be queried directly by configuring ModelarDB for querying (not ingestion).



**Running the image**


To run ModelarDB with this data ingested run

```shell
docker run -it --rm \
  -p 9999:9999 \
  --name modelardb \
  ghcr.io/modelardata/modelardb:latest <config-file>
```

If no config file is specified the default is to run it with `redd-query.conf`.

When the container is up and running try to query it with a simple select like

```shell
curl -d "select * from segment limit 3" -X POST localhost:9999
```



**Loading your own data and config**

To load your own data and config file you need to use Docker `bind-mount`.

An example of this is given below

```shell
docker run -it \
  -p 9999:9999 \
  --name modelardb \
  --mount type=bind,source=<path-to-your-data>,target=/data/,readonly \
  --mount type=bind,source=<path-to-config>,target=/conf/<config.conf>,readonly \
  ghcr.io/modelardata/modelardb:latest /conf/<config.conf>
```
where the config file you provide should contain the data path you chose as target above (line 4)
```txt
  # Supported: filepath (glob)
  source = "/data/*.csv"
```



## Config

ModelarDB is configured using a key-value format delimited by whitespace

```
modelardb.engine h2
modelardb.storage jdbc:h2:/path/to/modelardb.h2
modelardb.interface http
modelardb.time_zone UTC
```

The file `modelardb.conf` contains examples of all settings. If you just want to use ModelarDB with the [REDD](http://redd.csail.mit.edu/) dataset use the provided config file `redd-query.conf`.

Your config file can be put in either `$HOME/.modelardb.conf` or specified on the command-line when running ModelarDB.

Depending on whether you want to ingest new data or just want to query already ingested data there are a few settings you should pay attention to. We will now go through the most relevant.

### Configure for Ingestion

If you want to ingest your own data into ModelarDB the following settings are relevant

```
modelardb.storage jdbc:h2:file:./redd  	# Accepts a jdbc connection string
modelardb.ingestors 1  									# How many threads to use for ingestion
modelardb.source ./data/*.dat						# Directory where your data files are stored
```

Further you should make sure to fill out all the `modelardb.csv.*` settings so they match your data. Here is an example of the CSV settings available

```
## CSV Format Settings

# Supported: String
modelardb.csv.separator \s

# Suports: True / False
modelardb.csv.header False

# Supported: Java SimpleDateFormat, unix, or java
modelardb.csv.date_format unix

# Supported: An ISO 639 alpha-2 or alpha-3 language code, 
# or a language subtag up to 8 characters in length.
modelardb.csv.locale en
```

As an example the config file used for ingesting the [REDD](http://redd.csail.mit.edu/) dataset can be found in the `redd-ingest.conf` config file. You may also  have a look at this one to get a feel for the needed settings.



### Configure for Querying

If you just want to run ModelarDB with the included [REDD](http://redd.csail.mit.edu/)  dataset you can run it with the included `redd-query.conf` config file. If you have ingested your own data and now want to run ModelarDB in query only mode make sure to change the following settings

```
modelardb.storage jdbc:your-connection-string
modelardb.ingestors 0
# modelardb.source should be commented out
```



## Database Layout

ModelarDB stores data in 3 different tables and offers 2 view for querying the data

1. **Model type table**

   For storing information about the underlying model type used to compress the timeseries

2. **Timeseries table**

   For storing informatin about the ingested timeseries 

3. **Segment table** 

   For storing information about the compressed timeseries

4. **Segment view**

    For querying the compressed models
    
6. **Data Point view**

   For querying the datapoints (by reconstructing them from the models)



Below we present the DDL of the above mentioned tables/views so you can se precisely how the database schema is defined (please don't run the DDLs against ModelarDB as the tables are already in place). Additionally we also present examples of what the data in the tables look like (all data examples are taken from the ingested [REDD](http://redd.csail.mit.edu/) dataset).



### Model Type

**DDL**

```SQL
create table MODEL_TYPE
(
    MTID INTEGER,
    NAME VARCHAR
);
```



**Example Data**

| MTID | NAME                                                  |
| ---: | :---------------------------------------------------- |
|    1 | dk.aau.modelardb.core.models.UncompressedModelType    |
|    2 | dk.aau.modelardb.core.models.PMC_MeanModelType        |
|    3 | dk.aau.modelardb.core.models.SwingFilterModelType     |
|    4 | dk.aau.modelardb.core.models.FacebookGorillaModelType |



### Time Series

**DDL**

```SQL
create table TIME_SERIES
(
    TID               INTEGER,
    SCALING_FACTOR    REAL,
    SAMPLING_INTERVAL INTEGER,
    GID               INTEGER
);
```



**Example Data**

| TID  | SCALING_FACTOR | SAMPLING_INTERVAL | GID  |
| ---- | -------------- | ----------------- | ---- |
| 1    | 1              | 1000              | 1    |
| 2    | 1              | 1000              | 2    |
| 3    | 1              | 1000              | 3    |
| 4    | 1              | 1000              | 4    |



### Segment

**DDL**

```sql
create table SEGMENT
(
    GID        INTEGER,
    START_TIME BIGINT,
    END_TIME   BIGINT,
    MTID       INTEGER,
    MODEL      BINARY,
    GAPS       BINARY
);

create index SEGMENT_GID
    on SEGMENT (GID);

create index SEGMENT_START_TIME
    on SEGMENT (START_TIME);

create index SEGMENT_END_TIME
    on SEGMENT (END_TIME);
```



**Example Data**

| GID  | START_TIME    | END_TIME      | MTID | MODEL       | GAPS |
| ---- | ------------- | ------------- | ---- | ----------- | ---- |
| 1    | 1303132929000 | 1303133980000 | 2    | 435C962C... |      |
| 1    | 1303133981000 | 1303134030000 | 4    | 436191EC... |      |
| 1    | 1303134031000 | 1303134187000 | 2    | 4362170A... |      |
| 1    | 1303134188000 | 1303134237000 | 4    | 435EBD71... |      |
| ...  | ...           | ...           | ...  | ...         |      |
| 2    | 1303132929000 | 1303134326000 | 2    | 42EEE075... |      |
| 2    | 1303134327000 | 1303134376000 | 4    | 42CE3D71... |      |



### Segment View

The segment view is basically the same as the segment table with the one difference that it contains the `tid` (timeseries id) allowing you to query on this instead of the `gid` (group id). Further it comes with built-in user defined aggregate functions (UDAFs) allowing you to query the compressed models quickly and efficiently (see section on [querying](#query results) below for details).

**DDL**

```sql
create table SEGMENT
(
    tid INT, 
    start_time TIMESTAMP, 
    end_time TIMESTAMP, 
    mtid INT, 
    model BINARY, 
    gaps BINARY
);
```

(Note: if you have specified any `dimensions` in the config file they will be added as columns to this view. For more details on `dimensions` see the [troubleshooting](#troubleshooting) section at the end of the document)



**Example Data**

| TID  | START_TIME    | END_TIME      | MTID | MODEL       | GAPS             |
| ---- | ------------- | ------------- | ---- | ----------- | ---------------- |
| 1    | 1303132929000 | 1303133980000 | 2    | Q1yWLA==    | AAAAAQAAAAE...   |
| 1    | 1303133981000 | 1303134030000 | 4    | RABT19qd... | AAAAAQAAAAE...   |
| ...  | ...           | ...           | ...  | ...         | ...              |
| 2    | 1303132929000 | 1303134326000 | 2    | RFuVw8rX... | AAAAAQAAAAE...   |
| 2    | 1303134327000 | 1303134376000 | 4    | Q00mAg==    | AAAAAQAAAAE...   |




### Data Point View

ModelarDB has a datapoint view which you can use if you want to get back the decompressed data points. Please note that this reconstructs the datapoints using the model (and its parameters) stored in the `segment` table and hence if the model is lossy then the returned data point might deviate from the actual ingested data point up to the percentage specified in the config setting `modelardb.error_bound`. Further please note that quering this view is slower than using the `segment` view as it has to reconstruct the data points requested.

**DDL**

```sql
create table DATAPOINT
(
    tid INT, 
    timestamp TIMESTAMP, 
    value REAL
);
```



**Example data**

| TID  | TIMESTAMP           | VALUE     |
| ---- | ------------------- | --------- |
| 2    | 2011-04-18 13:22:09 | 119.43839 |
| 2    | 2011-04-18 13:22:10 | 119.43839 |
| 2    | 2011-04-18 13:22:11 | 119.43839 |



## Query

In this section we will go over the different interfaces through which ModelarDB can be queried, what the retuned results look like and the specialized operators available when querying.

### Interfaces

ModelarDB can be queried in 4 different ways by setting the config option `modelardb.interface` to one of

1. `REPL`
2. `path/to/file`
3. `HTTP`
4. `socket`

(Note: you can only query the `segment` and `datapoint` views)



The `REPL` option is the most simple one and just leaves a terminal open where you can input SQL.
The other options work as follows.



#### File

With the file option you put your SQL queries in a file. One query on each line.

```sql
SELECT count(*) FROM segment
SELECT * FROM segment LIMIT 5
```

And then point ModelarDB to the file

```
modelardb.interface ./queries.sql
```



Please note that the `file` option will make ModelarDB exit after execution.



#### HTTP

After setting ModelarDB to listen on the `HTTP` interface in the config file

```
modelardb.interface http
```

You can query it by

```sh
curl -X POST "http://localhost:9999" \
     -d "SELECT * FROM segment LIMIT 5"
```

ModelarDB listens on port `9999` (at the moment it is not possible to change this port)



#### Socket

After setting ModelarDB to listen on the `socket` interface in the config file

```
modelardb.interface socket
```

You can query it by using netcat 

```sh
echo -n "SELECT * FROM segment LIMIT 5;" | nc localhost 9999
```

ModelarDB listens on port `9999` (at the moment it is not possible to change this port)

Note: `SELECT` must be in all caps when using `socket` as an interface. Also getting the socket interface to work with the Docker images is more involved than the `HTTP` interface. So we recommend using the `HTTP` interface in this scenario.



### Query Results

At the moment the query results are returned in `JSON` format. The following is a demonstration of the different query operators and the returned result. The query run can be seen from the `query:` field in the JSON.



#### Segment Table

Here is the result of a simple `select` query directly on the `segment` table

```json
{
  "time": "PT0.003S",
  "query": "SELECT * FROM segment LIMIT 5",
  "result": [
    {
      "TID": 1,
      "START_TIME": "2011-04-18 13:22:09.0",
      "END_TIME": "2011-04-18 13:39:40.0",
      "MTID": 2,
      "MODEL": "Q1yWLA==",
      "GAPS": "AAAAAQAAAAEAAAAA"
    },
    {
      "TID": 1,
      "START_TIME": "2011-04-18 13:39:41.0",
      "END_TIME": "2011-04-18 13:40:30.0",
      "MTID": 4,
      "MODEL": "RABT19qdUjvQxwSW/AM09wPdPcApFtAPoVwAki0AlrfACeNQAGHcAWprAELhwHOj8BwYXACbhwB1wsARORAdg7QAlHcBy1NAc2LwCfl0ApsLADHjwACHMASW/ANbiwCiFMAJmbADIdwBYjUAZuLABysQHoN0B0t3AErdwGUN0AMyFADxyQHEAEAbR3AHdYQA4K0AR4VACY9wBZuMB1I9AdFswBsWwA==",
      "GAPS": "AAAAAQAAAAEAAAAA"
    },
    {
      "TID": 1,
      "START_TIME": "2011-04-18 13:40:31.0",
      "END_TIME": "2011-04-18 13:43:07.0",
      "MTID": 2,
      "MODEL": "ROD8kQ==",
      "GAPS": "AAAAAQAAAAEAAAAA"
    },
    {
      "TID": 1,
      "START_TIME": "2011-04-18 13:43:08.0",
      "END_TIME": "2011-04-18 13:43:57.0",
      "MTID": 4,
      "MODEL": "RFuVw8rXCKizABFvUAAhub0bIUxuFL1wsmyGjZDXpp2o4R72pZZm8ELIfshSsshvyN3o4WXi9YbkpyLbd0yOG4lN44hbGOFYlxvJ8byTG82p6dJ643FL//LkhzZnSInkbE//Jg3NIWQo0ORZpOrIABMAAJJunBjx4OpnHpCp2RbKesIIABEAAJr1hDyLQA==",
      "GAPS": "AAAAAQAAAAEAAAAA"
    },
    {
      "TID": 1,
      "START_TIME": "2011-04-18 13:43:58.0",
      "END_TIME": "2011-04-18 14:17:43.0",
      "MTID": 2,
      "MODEL": "Q00mAg==",
      "GAPS": "AAAAAQAAAAEAAAAA"
    }
  ]
}
```



Here is an example of doing a `count` on the `segment` table

```json
{
  "time": "PT0.804S",
  "query": "SELECT count(*) FROM segment",
  "result":  [
    {"COUNT(*)":"380744"}
  ]
}
```



There isn't much value in querying the `segment` view using just a simple SQL `select *` though. As this only returns the compressed models (and these mostly makes sense to ModelarDB). Instead use one of the built-in UDAFs which are able to operate directly on the compressed models.

* `count_s`
* `min_s`
* `max_s`
* `sum_s`
* `avg_s`

Examples of using these aggregate query operators are given below.
(Note: instead of providing all the column names [`tid`, `start_time`, `end_time`, `mtid`, `model`, `gaps`] you can just use the hashtag / pound sign `#`)

**Count_S**

```json
{
  "time": "PT0.965S",
  "query": "SELECT COUNT_S(#) FROM segment",
  "result":  [
    {"COUNT_S(TID, START_TIME, END_TIME)":"24677756"}
  ]
}
```

**Min_S**

```json
{
  "time": "PT0.046S",
  "query": "SELECT MIN_S(#) FROM segment WHERE tid = 1",
  "result":  [
    {"MIN_S(TID, START_TIME, END_TIME, MTID, MODEL, GAPS)":49.80843}
  ]
}
```

**Max_S**

```json
{
  "time": "PT0.039S",
  "query": "SELECT MAX_S(#) FROM segment WHERE tid = 1",
  "result":  [
    {"MAX_S(TID, START_TIME, END_TIME, MTID, MODEL, GAPS)":6081.36}
  ]
}
```

**Sum_S**

```json
{
  "time": "PT0.051S",
  "query": "SELECT SUM_S(#) FROM segment WHERE tid = 1",
  "result":  [
    {"SUM_S(TID, START_TIME, END_TIME, MTID, MODEL, GAPS)":3.54920928E8}
  ]
}
```

**Avg_S**

```json
{
  "time": "PT0.164S",
  "query": "SELECT AVG_S(#) FROM segment WHERE tid = 1",
  "result":  [
    {"AVG_S(TID, START_TIME, END_TIME, MTID, MODEL, GAPS)":227.27156}
  ]
}
```



#### Data Point View

As mentioned in the [data point view](#data-point-view-1) section above you can get the reconstructed datapoints by quering the view

```json
{
  "time": "PT0.009S",
  "query": "SELECT * FROM datapoint WHERE tid = 2 AND timestamp < '2011-04-18 16:45:18' limit 3",
  "result":  [
    {"TID":2,"TIMESTAMP":"2011-04-18 13:22:09.0","VALUE":119.43839},
    {"TID":2,"TIMESTAMP":"2011-04-18 13:22:10.0","VALUE":119.43839},
    {"TID":2,"TIMESTAMP":"2011-04-18 13:22:11.0","VALUE":119.43839}
  ]
}
```

Note here how the values returned are the same because the underlying sensor measurements didn't change (or were within the error bound).



You can run aggregate queries directly on the `datapoint` view but be aware that this is not as effective as using the built-in operators mentioned above. This is because when using the `datapoint` view, the points have to be recreated while this can be avoided when using the `segment` view with the built-in operators (since they are tailored to the compressed representation of the data).

```json
{
  "time": "PT0.79S",
  "query": "SELECT avg(value) FROM datapoint WHERE tid = 2 AND timestamp < '2011-04-18 16:45:18'",
  "result":  [
    {"AVG(VALUE)":49.169487}
  ]
}

```





## Troubleshooting

If you are having problems running ModelarDB make sure the following settings are commented out

```
# modelardb.source.derived
# modelardb.dimensions
# modelardb.correlation
```



They are more advanced and increase the complexity. For completeness we will quickly go through them here.

The `modelardb.source.derived` option allows you to create derived timeseries by specifying the `tid` or name of the source, the function to apply to the source and a new name for the derived timeseries. For instance

```
modelardb.source.derived 1 sin_ts sin(toRadians(value))
```

creates a new timeseries named `sin_ts` by applying the `sin` and `toRadian` function from `scala.math` to the timeseries with id=1.



The `modelardb.dimensions` option allows you to specify a hieracy for the ingested data.

For instance the setting

```
modelardb.dimensions Location, country string, region string, city string
```

defines a dimension named `Location` which has a hieracy of country > region > city. Such a defintion would be useful if your data contains information about the country, region and city where it is installed.

With this dimensionn defined you can the specify correlations between the different dimensions in the following way

```
# Data points are correlated if level 1 of the Location dimension (country) is France
modelardb.correlation Location 1 France 
# Data points are correlated if level 2 of the Location dimension (region) is Bordeaux
modelardb.correlation Location 2 Bordeaux 
```

  

You can also specify correlations independent of dimensions such as

```
modelardb.correlation auto
```

which tells ModelarDB to calculate the lowest non-zero distance possible in the dataset and use this value to group data points.

Alternatively you can specify the distance threshold manually as

```
modelardb.correlation 0.25
```

In ModelarDB distance is defined such that the more levels in the dimension hieracy the timeseries shares the "closer" they are to each other.

For the specific details of how dimensions and correlation works see section IV of

> ***Scalable Model-Based Management of Correlated Dimensional Time Series in ModelarDB+***
> by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen
> in *The Proceedings of ICDE, 1380-1391, 2021*

Links: [IEEE](https://ieeexplore.ieee.org/document/9458830), [arXiv](https://arxiv.org/abs/1903.10269)

The reason for specifying dimensions and correlations is that with this extra information ModelarDB can compress the data even more.
