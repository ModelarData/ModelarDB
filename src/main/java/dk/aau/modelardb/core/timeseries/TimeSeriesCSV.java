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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import dk.aau.modelardb.core.DataPoint;

public class TimeSeriesCSV extends TimeSeries {

    /** Constructors **/
    //Comma Separated Values
    public TimeSeriesCSV(String stringPath, int sid, int resolution,
                         String splitString, boolean hasHeader,
                         int timestampColumn, String dateFormat, String timeZone,
                         int valueColumn, String localeString) {
        super(stringPath.substring(stringPath.lastIndexOf('/') + 1), sid, resolution);
        this.stringPath = stringPath;

        //A small buffer is used so more time series can be ingested in parallel
        this.bufferSize = 1024;
        this.hasHeader = hasHeader;
        this.splitString = splitString;
        this.scalingFactor = 1.0F;

        this.timestampColumn = timestampColumn;
        switch (dateFormat) {
            case "unix":
                this.dateParserType = 1;
                break;
            case "java":
                this.dateParserType = 2;
                break;
            default:
                this.dateParserType = 3;
                this.dateParser = new SimpleDateFormat(dateFormat);
                this.dateParser.setTimeZone(java.util.TimeZone.getTimeZone(timeZone));
                this.dateParser.setLenient(false);
                break;
        }

        this.valueColumn = valueColumn;
        Locale locale = new Locale(localeString);
        this.valueParser = NumberFormat.getInstance(locale);
        this.decodeBuffer = new StringBuffer();
        this.nextBuffer = new StringBuffer();
    }

    /** Public Methods **/
    public void open() throws RuntimeException {
        try {
            FileChannel fc = FileChannel.open(Paths.get(this.stringPath));

            //Wraps the channel in a stream if the data is compressed
            String suffix = "";
            int lastIndexOfDot = this.stringPath.lastIndexOf('.');
            if (lastIndexOfDot > -1) {
                suffix = this.stringPath.substring(lastIndexOfDot);
            }
            stringPath = null;

            if (".gz".equals(suffix)) {
                InputStream is = Channels.newInputStream(fc);
                GZIPInputStream gis = new GZIPInputStream(is);
                this.channel = Channels.newChannel(gis);
            } else {
                this.channel = fc;
            }
            this.byteBuffer = ByteBuffer.allocate(this.bufferSize);
            if (this.hasHeader) {
                readLines();
            }
        } catch (IOException ioe) {
            //An unchecked exception is used so the function can be called in a lambda function
            throw new RuntimeException(ioe);
        }
    }

    public DataPoint next() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return nextDataPoint();
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public boolean hasNext() {
        try {
            if (this.nextBuffer.length() == 0) {
                readLines();
            }
            return this.nextBuffer.length() != 0;
        } catch (IOException ioe) {
            close();
            throw new java.lang.RuntimeException(ioe);
        }
    }

    public String toString() {
        return "Time Series: [" + this.sid + " | " + this.source + " | " + this.resolution + "]";
    }

    public void close() {
        //If the channel was never initialized there is nothing to close
        if (this.channel == null) {
            return;
        }

        try {
            this.channel.close();
            //Clears all references to channels and buffers to enable garbage collection
            this.byteBuffer = null;
            this.nextBuffer = null;
            this.channel = null;
        } catch (IOException ioe) {
            throw new java.lang.RuntimeException(ioe);
        }
    }

    /** Private Methods **/
    private void readLines() throws IOException {
        int lastChar;

        //Reads a whole line from the channel by looking for either a new line or if no additional bytes are returned
        do {
            this.byteBuffer.clear();
            this.channel.read(this.byteBuffer);
            lastChar = this.byteBuffer.position();
            this.byteBuffer.flip();
            this.decodeBuffer.append(StandardCharsets.UTF_8.decode(this.byteBuffer));
        } while (lastChar != 0 && this.decodeBuffer.indexOf("\n") == -1);

        //Transfer all fully read data points into a new buffer to simplify the remaining implementation
        int lastFullyParsedDataPoint = this.decodeBuffer.lastIndexOf("\n") + 1;
        this.nextBuffer.append(this.decodeBuffer, 0, lastFullyParsedDataPoint);
        this.decodeBuffer.delete(0, lastFullyParsedDataPoint);
    }

    private DataPoint nextDataPoint() throws IOException {
        try {
            int nextDataPointIndex = this.nextBuffer.indexOf("\n") + 1;
            String[] split;

            if (nextDataPointIndex == 0) {
                split = this.nextBuffer.toString().split(splitString);
            } else {
                split = this.nextBuffer.substring(0, nextDataPointIndex).split(splitString);
                this.nextBuffer.delete(0, nextDataPointIndex);
            }

            //Parses the timestamp column as either Unix time, Java time, or a human readable timestamp
            long timestamp = 0;
            switch (this.dateParserType) {
                case 1:
                    //Unix time
                    timestamp = new Date(Long.parseLong(split[timestampColumn]) * 1000).getTime();
                    break;
                case 2:
                    //Java time
                    timestamp = new Date(Long.parseLong(split[timestampColumn])).getTime();
                    break;
                case 3:
                    //Human readable timestamp
                    timestamp = dateParser.parse(split[timestampColumn]).getTime();
                    break;
            }
            float value = valueParser.parse(split[valueColumn]).floatValue();
            return new DataPoint(this.sid, timestamp, this.scalingFactor * value);
        } catch (ParseException pe) {
            //If the input cannot be parsed the stream is considered empty
            this.channel.close();
            throw new java.lang.RuntimeException(pe);
        }
    }

    /** Instance Variables **/
    private String stringPath;
    private boolean hasHeader;
    private float scalingFactor;
    private int bufferSize;
    private ByteBuffer byteBuffer;
    private StringBuffer decodeBuffer;
    private StringBuffer nextBuffer;
    private ReadableByteChannel channel;
    private String splitString;
    private int timestampColumn;
    private SimpleDateFormat dateParser;
    private int dateParserType;
    private int valueColumn;
    private NumberFormat valueParser;
}