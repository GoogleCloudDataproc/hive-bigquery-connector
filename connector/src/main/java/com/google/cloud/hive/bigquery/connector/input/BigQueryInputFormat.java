/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hive.bigquery.connector.input;

import java.io.IOException;
import java.util.List;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.arrow.ArrowRecordReader;
import com.google.cloud.hive.bigquery.connector.input.avro.AvroRecordReader;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.*;

public class BigQueryInputFormat implements InputFormat<NullWritable, ObjectWritable>, AcidInputFormat<NullWritable, ObjectWritable>, VectorizedInputFormatInterface {

    /**
     * Creates hadoop splits (i.e BigQuery streams) so that each task can read data from the
     * corresponding stream.
     *
     * @param jobConf The job's configuration
     * @param numSplits Number of splits requested by MapReduce, but ignored as BigQuery decides the
     *     number of streams used in the read session.
     * @return InputSplit[] - Collection of FileSplits
     */
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
        return BigQueryInputSplit.createSplitsFromBigQueryReadStreams(jobConf);
    }

    @Override
    public RecordReader<NullWritable, ObjectWritable> getRecordReader(
        InputSplit inputSplit, JobConf jobConf, Reporter reporter) {
        Injector injector = Guice.createInjector(new HiveBigQueryConnectorModule(jobConf));
        DataFormat readDataFormat = injector.getInstance(HiveBigQueryConfig.class).getReadDataFormat();
        if (readDataFormat.equals(DataFormat.ARROW)) {
            return new ArrowRecordReader((BigQueryInputSplit) inputSplit, jobConf);
        } else if (readDataFormat.equals(DataFormat.AVRO)) {
            return new AvroRecordReader((BigQueryInputSplit) inputSplit, jobConf);
        }
        throw new RuntimeException("Invalid readDataFormat: " + readDataFormat);

    }

    @Override
    public RowReader<ObjectWritable> getReader(InputSplit split, Options options) throws IOException {
        return null;
    }

    @Override
    public RawReader<ObjectWritable> getRawReader(Configuration conf, boolean collapseEvents, int bucket, ValidWriteIdList validWriteIdList, Path baseDirectory, Path[] deltaDirectory) throws IOException {
        return null;
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files) throws IOException {
        return false;
    }

    @Override
    public VectorizedSupport.Support[] getSupportedFeatures() {
        return new VectorizedSupport.Support[] {VectorizedSupport.Support.DECIMAL_64};
    }
}
