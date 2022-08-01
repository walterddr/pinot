package org.apache.pinot.plugin.inputformat.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;


public class DeltaRecordReaderTest extends AbstractRecordReaderTest {
  private final File _dataFile = new File(_tempDir, "delta-data");
  private final File _dataFilePq = new File(_tempDir, "delta-data.parquet");

  @Override
  protected RecordReader createRecordReader()
      throws Exception {
    DeltaRecordReader recordReader = new DeltaRecordReader();
    recordReader.init(_dataFile, _sourceFields, null);
    return recordReader;
  }

  @Override
  protected org.apache.pinot.spi.data.Schema getPinotSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName("SampleRecord")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("email", FieldSpec.DataType.STRING).build();
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    Schema schema = AvroUtils.getAvroSchemaFromPinotSchema(getPinotSchema());
    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> r : recordsToWrite) {
      GenericRecord record = new GenericData.Record(schema);
      for (FieldSpec fieldSpec : getPinotSchema().getAllFieldSpecs()) {
        record.put(fieldSpec.getName(), r.get(fieldSpec.getName()));
      }
      records.add(record);
    }

    try (ParquetWriter<GenericRecord> writer =
        ParquetUtils.getParquetAvroWriter(new Path(_dataFilePq.getAbsolutePath()), schema)) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }

    AddFile file = new AddFile(_dataFilePq.getPath(), new HashMap<>(), _dataFilePq.length(),
        System.currentTimeMillis(), true, null, null);
    List<Action> totalCommitFiles = new ArrayList<>();
    totalCommitFiles.add(file);

    DeltaLog log = DeltaLog.forTable(new Configuration(), _dataFile.getPath());
    OptimisticTransaction txn = log.startTransaction();
    Metadata metadata = Metadata.builder()
        .schema(toStructureType(schema))
        .build();
    txn.updateMetadata(metadata);
    txn.commit(totalCommitFiles, new Operation(Operation.Name.UPDATE), "Test");
  }

  private StructType toStructureType(Schema avroSchema) {
    List<StructField> fields = new ArrayList<>();
    for (Schema.Field field : avroSchema.getFields()) {
      fields.add(new StructField(field.name(), toDataType(field.schema())));
    }
    return new StructType(fields.toArray(new StructField[]{}));
  }

  private DataType toDataType(Schema schema) {
    switch (schema.getType()) {
      case STRING:
        return new StringType();
      case BYTES:
        return new ByteType();
      case INT:
        return new IntegerType();
      case LONG:
        return new LongType();
      case FLOAT:
        return new FloatType();
      case DOUBLE:
        return new DoubleType();
      case BOOLEAN:
        return new BooleanType();
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
    List<Object[]> expectedPrimaryKeys) throws IOException {

    // iterator each record, and compare field values for each record
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();

      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();

        if (fieldSpec.getDataType().getStoredType().equals(FieldSpec.DataType.INT)) {
          Assert.assertEquals(expectedRecord.get(fieldSpecName), actualRecord.getValue(fieldSpecName));
        }
      }
    }
  }
}
