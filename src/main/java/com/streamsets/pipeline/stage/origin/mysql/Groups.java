package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  MYSQLBINLOG("MySQLBinlog"),;

  private final String label;

  private Groups(String label) {
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return this.label;
  }
}
