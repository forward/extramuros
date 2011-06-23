package extramuros.java.formats;

import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.mahout.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Date;
import java.util.Iterator;

class RowIterator implements Iterator<Row> {

    protected Iterator<Pair<Writable,Writable>> reader;
    protected TableHeader header;
    protected Row lastRow;

    private static final Logger log = LoggerFactory.getLogger(RowIterator.class);

    RowIterator(Iterator<Pair<Writable,Writable>> reader, TableHeader header) {
        this.reader = reader;
        this.header = header;
        this.lastRow = null;
    }

    public boolean hasNext() {
       return reader.hasNext();
    }

    public Row next() {
        Pair<Writable, Writable> pair = reader.next();
        if(pair == null) {
            return null;
        } else {
           return (Row) pair.getSecond();
        }

    }

    public void remove() {
    }
}

/**
 * User: antonio
 * Date: 31/05/2011
 * Time: 12:14
 */
public class Table implements AbstractTable {

    private TableHeader header;
    private String rowsPath;
    protected Configuration config;
    protected String tablePath;

    public Table(TableHeader header, String rowsPath) {
        this.setHeader(header);
        this.setRowsPath(rowsPath);
    }

    public Table() {
    }

    public void write(DataOutput dataOutput) throws IOException {
        getHeader().write(dataOutput);
        Text tmp = new Text(getRowsPath());
        tmp.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        setHeader(new TableHeader());
        getHeader().readFields(dataInput);
        Text tmp = new Text();
        tmp.readFields(dataInput);
        setRowsPath(tmp.toString());
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }

    public Configuration getConfiguration() {
        if(config == null) {
            config = new Configuration(true);
            return config;
        } else {
            return config;
        }
    }

    public SequenceFile.Reader rowsReader() throws IOException {
        Path rowsInputFile = new Path(getRowsPath());
        FileSystem fs = FileSystem.get(getConfiguration());

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,rowsInputFile,config);

        return reader;
    }

    public Iterator<Row> iterator() {
        try {
            return new RowIterator(TableUtils.directorySeqIterator(new Path(getRowsPath()), config), getHeader());
        } catch (IOException e) {
            return null;
        }
    }

    public TableHeader getHeader() {
        return header;
    }

    public void setHeader(TableHeader header) {
        this.header = header;
    }

    public String getRowsPath() {
        return rowsPath;
    }

    public void setRowsPath(String rowsPath) {
        this.rowsPath = rowsPath;
    }

    public String getTablePath() {
        if(tablePath != null) {
            return tablePath;
        } else {
            return this.rowsPath.split(".row")[0];
        }
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public boolean isAdapter() {
        return false;
    }

    public AbstractTable clone() {
        Table cloned = new Table();
        cloned.setHeader(getHeader().clone());
        cloned.setConfiguration(new Configuration(getConfiguration()));
        cloned.setRowsPath(new String(getRowsPath()));

        return cloned;
    }

    public boolean save() {
        try {
            TableUtils.writeSingleWritable(new Path(getTablePath()),new LongWritable((new Date()).getTime()),this,getConfiguration());
            return true;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public Class<? extends Writable> getRowClass() {
        return Row.class;
    }
}
