package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MYSQL_001("Error: {}"),;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return null;
  }

  @Override
  public String getMessage() {
    return null;
  }
}
