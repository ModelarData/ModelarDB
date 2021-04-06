/* Copyright 2018-2020 Aalborg University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.core.timeseries;

import dk.aau.modelardb.core.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class TimeSeriesParquet extends TimeSeries {
    //TODO: Uses dependencies brought in by Spark, decide if it acceptable that Core has dependencies
    /** Public Methods **/
    public TimeSeriesParquet(String stringPath, int sid, int resolution, int timestampColumnIndex, int valueColumnIndex) {
        super(stringPath.substring(stringPath.lastIndexOf('/') + 1), sid, resolution);
        this.stringPath = stringPath;
        this.timestampColumnIndex = timestampColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
    }

    public void open() {
        try {
            Path path = new Path(this.stringPath);
            InputFile iff = HadoopInputFile.fromPath(path, new Configuration());
            ParquetReadOptions pro = ParquetReadOptions.builder().build();
            this.fileReader = new ParquetFileReader(iff, pro);
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public DataPoint next() {
        SimpleGroup rowGroup = (SimpleGroup) this.recordReader.read();
        long timestamp = rowGroup.getLong(this.timestampColumnIndex, 0) / 1000;
        float value = rowGroup.getFloat(this.valueColumnIndex, this.rowIndex);
        this.rowIndex++;
        return new DataPoint(this.sid, timestamp, this.scalingFactor * value);
    }

    public boolean hasNext() {
        try {
            if (this.rowIndex != this.rowCount && this.rowCount != 0) {
                return true;
            }

            PageReadStore readStore = this.fileReader.readNextRowGroup();
            if (readStore != null) {
                MessageType schema = this.fileReader.getFooter().getFileMetaData().getSchema();
                GroupRecordConverter grc = new GroupRecordConverter(schema);
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                this.recordReader = columnIO.getRecordReader(readStore, grc);
                this.rowIndex = 0;
                this.rowCount = readStore.getRowCount();
                return true;
            } else {
                return false;
            }
        } catch (IOException ioe) {
            close();
            throw new RuntimeException(ioe);
        }
    }

    public String toString() {
        return "Time Series: [" + this.sid + " | " + this.source + " | " + this.resolution + "]";
    }

    public void close() {
        try {
            this.fileReader.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** Instance Variables **/
    private final String stringPath;
    private final int timestampColumnIndex;
    private final int valueColumnIndex;

    private int rowIndex;
    private long rowCount;
    private ParquetFileReader fileReader;
    private RecordReader recordReader;
}