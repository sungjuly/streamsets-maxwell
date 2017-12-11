package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class MySQLBinlogSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLBinlogSource.class);

  public abstract MySQLBinlogSourceConfig getConfig();

  private Maxwell maxwell;
  private Thread maxwellThread;
  private ArrayBlockingQueue<Record> queue = new ArrayBlockingQueue<>(1000);

  class MySQLBinlogProducerFactory implements ProducerFactory {
    @Override
    public AbstractProducer createProducer(MaxwellContext context) {
      return new MySQLBinlogProducer(context, queue) {
        @Override
        public Record createRecord(String recordSourceId) {
          return getContext().createRecord(recordSourceId);
        }
      };
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    return issues;
  }

  private void initializeMaxwell(String lastSourceOffset) {
    try {
      Position position = null;
      if (lastSourceOffset != null && !"".equals(lastSourceOffset)) {
        LOG.info("Initializing with " + lastSourceOffset + " offset");
        String[] info = lastSourceOffset.split(":");
        String file = info[0];
        Long offset = Long.parseLong(info[1]);
        position = new Position(BinlogPosition.at(offset, file), 0L);
      }
      String[] args = {"--config", getConfig().pathConfig};
      MaxwellConfig config = new MaxwellConfig(args);
      // To respect custom producer factory class first
      config.producerFactory = new MySQLBinlogProducerFactory();
      if (position != null) {
        config.initPosition = position;
      }
      maxwell = new Maxwell(config);
      maxwellThread = new Thread(maxwell);
      maxwellThread.setDaemon(false);
      maxwellThread.start();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void destroy() {
    if (maxwell != null) {
      maxwell.terminate();
      maxwell = null;
    }
    if (maxwellThread != null) {
      maxwellThread.interrupt();
      maxwellThread = null;
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    if (maxwell == null || maxwellThread == null) {
      initializeMaxwell(lastSourceOffset);
    }
    String nextSourceOffset = "";
    if (lastSourceOffset != null)
      nextSourceOffset = lastSourceOffset;

    int numRecords = 0;
    while (numRecords < maxBatchSize) try {
      Record record = queue.take();
      nextSourceOffset = record.getHeader().getSourceId();
      batchMaker.addRecord(record);
      numRecords++;
    } catch (InterruptedException e) {
    }
    return nextSourceOffset;
  }
}
