package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.*;

@StageDef(
    version = 1,
    label = "MySQL Binlog Origin",
    description = "This origin leverages Maxwell, that is open source project from zendesk, to ingest MySQL binlog data",
    icon = "mysql.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    onlineHelpRefUrl = "http://github.com/sungjuly/streamsets-maxwell"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class MySQLBinlogDSource extends MySQLBinlogSource {

  @ConfigDefBean
  public MySQLBinlogSourceConfig config;

  @Override
  public MySQLBinlogSourceConfig getConfig() {
    return config;
  }
}
