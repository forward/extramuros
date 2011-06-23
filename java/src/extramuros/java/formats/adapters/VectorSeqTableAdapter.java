package extramuros.java.formats.adapters;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.TableHeader;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;


class VectorSequenceRowIterator implements Iterator<Row> {

    protected  Iterator<Pair<Writable,Writable>> reader;
    protected Path[] inputFiles;
    protected VectorSeqTableAdapter adapter;
    protected Row lastRow;
    protected int currentFile;
    protected FileSystem fs;
    protected int counter;

    private static final Logger log = LoggerFactory.getLogger(VectorSequenceRowIterator.class);

    VectorSequenceRowIterator(Path input, VectorSeqTableAdapter adapter) throws IOException {
        this.adapter = adapter;
        this.lastRow = null;

        // Check files
        fs = FileSystem.get(adapter.getConfiguration());
        if(fs.isFile(input)) {
            inputFiles = new Path[1];
            inputFiles[0] = input;
        } else {
            FileStatus[] children = fs.listStatus(input);
            ArrayList<Path> childrenPaths = new ArrayList<Path>(children.length);
            for(FileStatus child : children) {
                if(fs.isFile(child.getPath())) {
                    childrenPaths.add(child.getPath());
                }
            }

            inputFiles = childrenPaths.toArray(new Path[childrenPaths.size()]);
        }

        currentFile = 0;
        counter = -1;
        reader = TableUtils.fileSeqIterator(inputFiles[currentFile],adapter.getConfiguration());
    }

    public boolean hasNext() {
        if(lastRow != null) {
            return true;
        } else {
            lastRow = readNextRow();
            return lastRow != null;
        }
    }

    private Row readNextRow() {
        try {
            if(reader.hasNext()) {
                Pair<Writable, Writable> pair = reader.next();
                Writable secondComponent = pair.getSecond();
                return adapter.map(pair.getFirst(),secondComponent);
            } else {
                currentFile++;
                if(currentFile < inputFiles.length) {
                    reader = TableUtils.fileSeqIterator(inputFiles[currentFile],adapter.getConfiguration());
                    return readNextRow();
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            log.error("IOException ERROR READING ROW",e);
            return null;
        }
    }

    public Row next() {
        if(lastRow!=null){
            Row tmp = lastRow;
            lastRow = null;
            return tmp;
        } else {
            return readNextRow();
        }
    }

    public void remove() {
    }
}

/**
 * User: antonio
 * Date: 06/06/2011
 * Time: 14:24
 */
public class VectorSeqTableAdapter extends AbstractTableAdapter<Writable, Double> implements AbstractTable {
    private TableHeader header;
    private String rowsPath;
    protected Configuration config;
    protected String tablePath;

    public VectorSeqTableAdapter() {
        // default constructor
    }

    public VectorSeqTableAdapter(TableHeader header, String rowsPath) {
        this.header = header;
        this.rowsPath = rowsPath;
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }

    public Configuration getConfiguration() {
        return config;
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
        if(tablePath == null) {
            return rowsPath+".tbl";
        } else {
            return tablePath;
        }
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public boolean isAdapter() {
        return true;
    }

    public AbstractTable clone() {
        VectorSeqTableAdapter adapter = new VectorSeqTableAdapter();

        adapter.setHeader(getHeader().clone());
        adapter.setRowsPath(new String(getRowsPath()));
        adapter.setConfiguration(new Configuration(getConfiguration()));
        adapter.setTablePath(new String(getTablePath()));

        return adapter;
    }

    public boolean save() {
        try {
            TableUtils.writeSingleWritable(new Path(getTablePath()), new LongWritable((new Date()).getTime()), this, getConfiguration());
            return true;
        } catch (IOException e) {
            log.error("Error saving table adapter",e);
            return false;
        }
    }

    public Class<? extends Writable> getRowClass() {
        return VectorWritable.class;
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

    @Override
    public Row parse(int id, Double[] parts) {
        Object[] values = new Object[parts.length];

        for (String columnName : header.getColumnNames()) {
            int position = header.positionFor(columnName);
            int type = header.typeFor(columnName);
            Double part = parts[position];
            try {
                if (type == RowTypes.DOUBLE) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        Double number = part;
                        values[position] = number;
                    }
                } else if (type == RowTypes.FLOAT) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        Float number = new Float(part.floatValue());
                        values[position] = number;
                    }
                } else if (type == RowTypes.INTEGER) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        Integer number = new Integer(part.intValue());
                        values[position] = number;
                    }
                } else if (type == RowTypes.LONG) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        Long number = new Long(part.longValue());
                        values[position] = number;
                    }
                } else {
                    values[position] = part.toString();
                }
            } catch (Exception ex) {
                values[position] = null;
                log.error("Error position in line.", ex);
            }
        }

        ArrayList<Object> rowValues = new ArrayList<Object>(values.length);
        for(Object value : values){
            rowValues.add(value);
        }
        return new Row(id, header.getColumnNames(),header.getColumnTypes(),rowValues);
    }

    @Override
    public Class<? extends InputFormat> inputFormat() {
        return SequenceFileInputFormat.class;
    }

    @Override
    public Row map(Writable key, Writable value) {
        Integer id = TableUtils.parseWritableKeyId(key);
        if(id==null) {
            id = TableUtils.randomId();
        }
        return parse(id,split(value));
    }

    public Iterator<Row> iterator() {
        try {
            return new VectorSequenceRowIterator(new Path(getRowsPath()),this);
        } catch (IOException e) {
            log.error("Error creating rows iterator", e);
            return null;
        }
    }

    public Double[] split(Vector input) {
        Double[] parts = new Double[input.getNumNondefaultElements()];
        for(int i=0; i<parts.length; i++) {
            parts[i] = input.get(i);
        }

        return parts;
    }

    public boolean isNull(Double part) {
        return part == null;
    }

    public boolean skip(Writable line) {
        return false;
    }

    public Double[] split(Writable input) {
        if(input instanceof VectorWritable) {
            return this.split(((VectorWritable) input).get());
        } else {
            return this.split(((WeightedVectorWritable) input).getVector());
        }
    }
}
