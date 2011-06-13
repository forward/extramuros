package extramuros.java.formats;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;

/**
 * User: antonio
 * Date: 31/05/2011
 * Time: 11:37
 */
public class Row implements Writable, WritableComparable<Row> {

    private int id;
    private ArrayList<String> columnsNames;
    private ArrayList<Integer> columnsTypes;
    private ArrayList<Object> values;

    public Row() {

    }

    public Row(int id, ArrayList<String> columnsNames, ArrayList<Integer> columnsTypes, ArrayList<Object> values) {
        setId(id);
        this.setColumnsNames(columnsNames);
        this.setColumnsTypes(columnsTypes);
        this.setValues(values);
    }

    public Row(int id, ArrayList<Object> values) {
        setId(id);
        setValues(values);
    }

    public void set(Row anotherRow) {
        setId(anotherRow.getId());
        setColumnsNames(anotherRow.getColumnsNames());
        setColumnsTypes(anotherRow.getColumnsTypes());
        setValues(anotherRow.getValues());
    }

    public void setSchema(ArrayList<String> columnsNames, ArrayList<Integer> columnsTypes) {
        setColumnsNames(columnsNames);
        setColumnsTypes(columnsTypes);
    }

    // serialization

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(getId());
        dataOutput.writeInt(values.size());

        for (int i = 0; i < values.size(); i++) {
            Object value = getValues().get(i);
            int type = getColumnsTypes().get(i);

            if (value == null) {
                dataOutput.writeByte(RowTypes.NULL);
                dataOutput.writeByte(type);
                NullWritable tmp = NullWritable.get();
                tmp.write(dataOutput);
            } else {
                dataOutput.writeByte(type);
                if (type == RowTypes.STRING || type == RowTypes.CATEGORICAL) {
                    Text tmp = new Text((String) value);
                    tmp.write(dataOutput);
                } else if (type == RowTypes.DOUBLE) {
                    DoubleWritable tmp = new DoubleWritable((Double) value);
                    tmp.write(dataOutput);
                } else if (type == RowTypes.LONG) {
                    LongWritable tmp = new LongWritable((Long) value);
                    tmp.write(dataOutput);
                } else if (value.getClass() == Float.class) {
                    FloatWritable tmp = new FloatWritable((Float) value);
                    tmp.write(dataOutput);
                } else if (type == RowTypes.INTEGER) {
                    IntWritable tmp = new IntWritable((Integer) value);
                    tmp.write(dataOutput);
                }
            }
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        setId(dataInput.readInt());
        int size = dataInput.readInt();
        values = new ArrayList<Object>(size);
        columnsTypes = new ArrayList<Integer>(size);
        boolean shouldCreateColumnNames = false;
        if (columnsNames == null) {
            columnsNames = new ArrayList<String>(size);
            shouldCreateColumnNames = true;
        }

        for (int i = 0; i < size; i++) {

            int type = (int) dataInput.readByte();
            if (type == RowTypes.NULL) {
                type = (int) dataInput.readByte();
                columnsTypes.add(type);
                if (shouldCreateColumnNames) {
                    columnsNames.add("DIM-" + i);
                }
                NullWritable tmp = NullWritable.get();
                tmp.readFields(dataInput);
                values.add(null);
            } else {
                columnsTypes.add(type);
                if (shouldCreateColumnNames) {
                    columnsNames.add("DIM-" + i);
                }

                if (type == RowTypes.DOUBLE) {
                    DoubleWritable tmp = new DoubleWritable();
                    tmp.readFields(dataInput);
                    values.add(tmp.get());
                } else if (type == RowTypes.FLOAT) {
                    FloatWritable tmp = new FloatWritable();
                    tmp.readFields(dataInput);
                    values.add(tmp.get());
                } else if (type == RowTypes.INTEGER) {
                    IntWritable tmp = new IntWritable();
                    tmp.readFields(dataInput);
                    values.add(tmp.get());
                } else if (type == RowTypes.LONG) {
                    LongWritable tmp = new LongWritable();
                    tmp.readFields(dataInput);
                    values.add(tmp.get());
                } else if (type == RowTypes.STRING || type == RowTypes.CATEGORICAL) {
                    Text tmp = new Text();
                    tmp.readFields(dataInput);
                    values.add(tmp.toString());
                }
            }
        }
    }

    public int compareTo(Row row) {
        if (getValues().size() < row.getValues().size()) {
            return -1;
        } else if (getValues().size() > row.getValues().size()) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean isNullAt(int position) {
        return getValues().get(position) == null;
    }

    // Fields

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ArrayList<String> getColumnsNames() {
        return columnsNames;
    }

    public void setColumnsNames(ArrayList<String> columnsNames) {
        this.columnsNames = columnsNames;
    }

    public ArrayList<Integer> getColumnsTypes() {
        return columnsTypes;
    }

    public void setColumnsTypes(ArrayList<Integer> columnsTypes) {
        this.columnsTypes = columnsTypes;
    }

    public ArrayList<Object> getValues() {
        return values;
    }

    public void setValues(ArrayList<Object> values) {
        this.values = values;
    }

    public Object valueForColumn(String columnName) {
        int idx = this.columnsNames.indexOf(columnName);
        return values.get(idx);
    }
}
