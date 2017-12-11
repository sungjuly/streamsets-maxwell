package com.streamsets.pipeline.stage.origin.mysql;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.columndef.*;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.streamsets.pipeline.api.Field.create;

public abstract class MySQLBinlogProducer extends AbstractProducer {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLBinlogProducer.class);
  private DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  private DateTimeFormatter fmt2 = DateTimeFormat.forPattern("yyyy-MM-dd");
  private ArrayBlockingQueue<Record> queue;

  public MySQLBinlogProducer(MaxwellContext context, ArrayBlockingQueue<Record> queue) {
    super(context);
    this.queue = queue;
  }

  public abstract Record createRecord(String recordSourceId);

  @Override
  public void push(RowMap r) throws Exception {
    BinlogPosition binlogPosition = r.getPosition().getBinlogPosition();
    String position = binlogPosition.getFile() + ":" + binlogPosition.getOffset();
    Record record = createRecord(position);
    String type = null;
    switch (r.getRowType().toLowerCase()) {
      case "insert":
        type = String.valueOf(OperationType.INSERT_CODE);
        break;
      case "update":
        type = String.valueOf(OperationType.UPDATE_CODE);
        break;
      case "delete":
        type = String.valueOf(OperationType.DELETE_CODE);
        break;
    }
    if (type != null)
      record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, type);
    record.getHeader().setAttribute("database", r.getDatabase());
    record.getHeader().setAttribute("table", r.getTable());
    Map<String, Field> fields = new HashMap<>();
    fields.put("operation", Field.create(r.getRowType().toLowerCase()));
    fields.put("operation_at", Field.create(r.getTimestampMillis()));
    if (r instanceof DDLMap) {
      fields.put("sql", Field.create(((DDLMap) r).getSql()));
    }
    r.getData().forEach((k, v) -> {
      fields.put(k, this.toField(r.getDataColumnDef(k), v));
    });
    record.set(create(fields));
    try {
      if (queue.offer(record, 50, TimeUnit.MILLISECONDS)) {
        this.setPosition(r);
      }
    } catch (InterruptedException e) {
    }
  }

  protected void setPosition(RowMap r) {
    context.setPosition(r);
  }

  public Field toField(ColumnDef d, Object v) {
    if (d instanceof IntColumnDef)
      return Field.create(Field.Type.LONG, v);
    else if (d instanceof BigIntColumnDef)
      return Field.create(Field.Type.LONG, v);
    else if (d instanceof FloatColumnDef || d instanceof FloatColumnDef)
      return Field.create(Field.Type.FLOAT, v);
    else if (d instanceof DecimalColumnDef)
      return Field.create(Field.Type.DECIMAL, v);
    else if (d instanceof DateTimeColumnDef || d instanceof DateColumnDef) {
      if (v != null) {
        try {
          DateTime dt = DateTime.parse((String) v, fmt.withZoneUTC());
          v = dt.getMillis();
        } catch (IllegalArgumentException e) {
          DateTime dt = DateTime.parse((String) v, fmt2.withZoneUTC());
          v = dt.getMillis();
        }
      }
      return Field.create(Field.Type.DATETIME, v);
    } else
      return Field.create(Field.Type.STRING, v);
  }
}
