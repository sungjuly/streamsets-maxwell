package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.ConfigDef;

public class MySQLBinlogSourceConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "default",
      label = "Maxwell configuration path",
      group = "MYSQLBINLOG"
  )
  public String pathConfig;
}
