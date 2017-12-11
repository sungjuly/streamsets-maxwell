# MySQL binlog origin on StreamSets

This consumes MySQL binlog on StreamSets using Maxwell (https://github.com/zendesk/maxwell).
This custom origin will represent `Record` object include `operation` and `operation_at` fields

Maxwell is the key technology to ingest MySQL binlog, but fewer APIs are needed to be open internally. 
You should check out this following project to build/install this custom origin.
  
* https://github.com/sungjuly/maxwell/tree/wip-for-StreamSets-origin

## Problems of built-in MySQL binlog lib from StreamSets

The `DataDollector` of StreamSets has own MySQL binlog origin, unfortunately, it has technical issues. You can check codebase at here - https://github.com/StreamSets/datacollector/tree/master/mysql-binlog-lib

Here are problems as experienced.

* Not able to represent unsigined/sigined value properly since MySQL binlog doesn't have any information about whether column is unsigned or not - https://github.com/shyiko/mysql-binlog-connector-java/issues/188
  A lot of mysql binlog client have the same issue and try to fix by combining `information_schema` db in MySQL
* Ignores events of schema evolution in built-in project
* Cannot resolve schema changes perfectly, built-in MySQL binlog lib doesn't have historical schema evolution.
  It caches table information in memory and it can be reset when restarting pipeline/StreamSets or whatever unexpected situations.
  More clearly, it means MySQL binlog event can be failed when schema information is not in table cache. That's not robust in my case  

## How to build

Please read this tutorial how to create a custom StreamSets origin first - https://github.com/StreamSets/tutorials/blob/master/tutorial-origin/readme.md

```bash
$ git checkout https://github.com/sungjuly/maxwell
$ cd maxwell && git checkout wip-for-StreamSets-origin
$ mvn clean install -DskipTests=true

$ git checkout https://github.com/sungjuly/steamsets-maxwell
$ cd steamsets-maxwell
$ mvn clean install -DskipTests=true
```

## Known issues

* Please don't do enforce stop in running the pipeline, the StreamSets will never stop your origin enforcedly, enforce stop means just changes of status of the pipeline.
  It's possible `DataDollector` of StreamSets will be restarted since MySQL binlog client already connected.