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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

class TextLinesRowIterator implements Iterator<Row> {

    protected LineReader reader;
    protected Path[] inputFiles;
    protected TextFileTableAdapter adapter;
    protected Row lastRow;
    protected int currentFile;
    protected FileSystem fs;
    protected int counter;

    private static final Logger log = LoggerFactory.getLogger(TextLinesRowIterator.class);

    TextLinesRowIterator(Path input, TextFileTableAdapter adapter) throws IOException {
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
        reader = new LineReader(fs.open(inputFiles[currentFile]));
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
            Text text = new Text();

            if(reader.readLine(text)>0) {
                counter++;
                String tmp = text.toString().trim();
                text = new Text(tmp.trim());
                return adapter.parseLine(counter, text);
            } else {
                reader.close();
                currentFile++;
                if(currentFile < inputFiles.length) {
                    reader = new LineReader(fs.open(inputFiles[currentFile]));
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
 * Date: 03/06/2011
 * Time: 08:48
 */
public class TextFileTableAdapter extends AbstractTableAdapter<Text, String> implements AbstractTable {

    private TableHeader header;
    private String rowsPath;
    protected Configuration config;
    protected String defaultSeparator;
    protected String tablePath;
    private String[] nullValues;

    public TextFileTableAdapter() {
        // default constructor
    }

    public TextFileTableAdapter(TableHeader header, String rowsPath) {
        this.header = header;
        this.rowsPath = rowsPath;
    }


    public String getDefaultSeparator() {
        return defaultSeparator;
    }

    public void setDefaultSeparator(String defaultSeparator) {
        this.defaultSeparator = defaultSeparator;
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
        TextFileTableAdapter adapter = new TextFileTableAdapter();

        adapter.setHeader(getHeader().clone());
        adapter.setRowsPath(new String(getRowsPath()));
        adapter.setConfiguration(new Configuration(getConfiguration()));
        adapter.setDefaultSeparator(new String(getDefaultSeparator()));
        adapter.setNullValues(getNullValues().clone());
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

    public void write(DataOutput dataOutput) throws IOException {
        getHeader().write(dataOutput);

        Text tmp = new Text(getRowsPath());
        tmp.write(dataOutput);

        tmp = new Text(defaultSeparator);
        tmp.write(dataOutput);

        dataOutput.writeInt(nullValues.length);
        for(String nullValue : nullValues) {
            tmp = new Text(nullValue);
            tmp.write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        setHeader(new TableHeader());
        getHeader().readFields(dataInput);

        Text tmp = new Text();
        tmp.readFields(dataInput);
        setRowsPath(tmp.toString());

        tmp = new Text();
        tmp.readFields(dataInput);
        setDefaultSeparator(tmp.toString());

        int numNullValues = dataInput.readInt();
        nullValues = new String[numNullValues];
        for(int i=0; i<nullValues.length; i++) {
            tmp = new Text();
            tmp.readFields(dataInput);
            nullValues[i] = tmp.toString();
        }
    }

    public Iterator<Row> iterator() {
        try {
            return new TextLinesRowIterator(new Path(getRowsPath()),this);
        } catch (IOException e) {
            log.error("Error creating rows iterator", e);
            return null;
        }
    }

    @Override
    public Row parse(int id, String[] parts) {
        Object[] values = new Object[parts.length];

        for (String columnName : header.getColumnNames()) {
            int position = header.positionFor(columnName);
            int type = header.typeFor(columnName);
            String part = parts[position];
            try {
                if (type == RowTypes.DOUBLE) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        part = part.replaceAll(" ","");
                        Double number = Double.parseDouble(part);
                        values[position] = number;
                    }
                } else if (type == RowTypes.FLOAT) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        part = part.replaceAll(" ","");
                        Float number = Float.parseFloat(part);
                        values[position] = number;
                    }
                } else if (type == RowTypes.INTEGER) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        part = part.replaceAll(" ","");
                        Integer number = Integer.parseInt(part);
                        values[position] = number;
                    }
                } else if (type == RowTypes.LONG) {
                    if(part == null) {
                        values[position] = null;
                    } else {
                        part = part.replaceAll(" ","");
                        Long number = Long.parseLong(part);
                        values[position] = number;
                    }
                } else {
                    values[position] = part;
                }
            } catch (Exception ex) {
                values[position] = null;
                log.error("Error parsing column in line.", ex);
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
        return TextInputFormat.class;
    }

    @Override
    public Row map(Writable key, Text value) {
        LongWritable keyWritable = (LongWritable) key;
        return parseLine(new Long(keyWritable.get()).intValue(), value);
    }


    protected void map(Writable key, Text line, Context context) throws IOException, InterruptedException {
        try {

            LongWritable keyWritable = (LongWritable) key;
            Long keyValue = keyWritable.get();
            Row row = parseLine(keyValue.intValue(),line);

            context.write(key,row);

        } catch(Exception e) {
            log.error("Error retrieving value from row",e);
        }
    }

    public boolean skip(Text line) {
        return false;
    }

    public String[] split(Text input) {
        String[] parts = new String[getHeader().getColumnNames().size()];
        String[] tmp = input.toString().split(defaultSeparator);
        for(int i=0; i<parts.length; i++) {
            if(i<tmp.length) {
                parts[i] = tmp[i];
            } else {
                parts[i] = null;
            }
        }
        return parts;
    }

    public String[] getNullValues() {
        return nullValues;
    }

    public void setNullValues(String[] nullValues) {
        this.nullValues = nullValues;
    }

    public boolean isNull(String part) {
        if(part == null) {
            return true;
        }

        for(String nullValue : nullValues) {
            if(part.replaceAll(" ","").compareToIgnoreCase(nullValue)==0) {
                return true;
            }
        }
        return false;
    }
}
