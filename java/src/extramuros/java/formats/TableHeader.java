package extramuros.java.formats;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 10:46
 */
public class TableHeader implements Writable {
    private ArrayList<String> columnNames;
    private HashMap<String,String> dateFormats;
    private ArrayList<Integer> columnTypes;
    private HashMap<String, Integer> columnsMap;

    public TableHeader() {

    }

    public TableHeader(ArrayList<String> columnNames, ArrayList<Integer> columnTypes) {
        this.setColumnNames(columnNames);
        this.setColumnTypes(columnTypes);
        this.setDateFormats(new HashMap<String, String>());
        buildColumnsMap();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(getColumnNames().size());
        for(int i=0; i< getColumnNames().size(); i++) {
            Text tmpName = new Text(getColumnNames().get(i));
            IntWritable tmpType = new IntWritable(getColumnTypes().get(i));
            tmpName.write(dataOutput);
            tmpType.write(dataOutput);
        }
        dataOutput.writeInt(getDateFormats().size());
        for(String columnName : getDateFormats().keySet()) {
            Text dateColumnName = new Text(columnName);
            dateColumnName.write(dataOutput);
            Text dateColumnFormat = new Text(getDateFormats().get(columnName));
            dateColumnFormat.write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        setColumnNames(new ArrayList<String>(size));
        setColumnTypes(new ArrayList<Integer>(size));

        for(int i=0; i<size; i++) {
            Text tmpName = new Text();
            IntWritable tmpType = new IntWritable();
            tmpName.readFields(dataInput);
            tmpType.readFields(dataInput);

            getColumnNames().add(tmpName.toString());
            getColumnTypes().add(tmpType.get());
        }

        size = dataInput.readInt();
        setDateFormats(new HashMap<String, String>(size));
        for(int i=0; i<size; i++) {
            Text tmpName = new Text();
            Text tmpFormat = new Text();

            tmpName.readFields(dataInput);
            tmpFormat.readFields(dataInput);

            getDateFormats().put(tmpName.toString(),tmpFormat.toString());
        }

        buildColumnsMap();
    }

    protected void buildColumnsMap() {
       setColumnsMap(new HashMap<String, Integer>(getColumnNames().size()));
        for(int i=0; i< getColumnNames().size(); i++) {
            getColumnsMap().put(getColumnNames().get(i), getColumnTypes().get(i));
        }
    }

    public int positionFor(String columnName){
        for(int i=0; i<getColumnNames().size(); i++) {
            if(getColumnNames().get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }

        return -1;
    }

    public boolean isColumnNumeric(int i) {
        int type = getColumnTypes().get(i);
        if(type == RowTypes.CATEGORICAL || type == RowTypes.STRING || type == RowTypes.NULL) {
            return false;
        } else {
            return true;
        }
    }

    public TableHeader clone() {
        ArrayList<String> clonedColumnNames = new ArrayList<String>(getColumnNames().size());
        ArrayList<Integer> clonedColumnTypes = new ArrayList<Integer>(getColumnTypes().size());

        for(int i=0; i<columnNames.size(); i++) {
            clonedColumnNames.add(new String(getColumnNames().get(i)));
            clonedColumnTypes.add(new Integer(getColumnTypes().get(i)));
        }
        TableHeader cloned = new TableHeader(clonedColumnNames, clonedColumnTypes);

        HashMap<String,String> clonedDateFormats = new HashMap<String, String>(getDateFormats().size());
        for(String key : getDateFormats().keySet()) {
            clonedDateFormats.put(new String(key), new String(getDateFormats().get(key)));
        }
        cloned.setDateFormats(clonedDateFormats);

        return cloned;
    }

    public int typeFor(String columnName) {
        return getColumnsMap().get(columnName);
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }

    public ArrayList<Integer> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(ArrayList<Integer> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public HashMap<String, Integer> getColumnsMap() {
        return columnsMap;
    }

    public void setColumnsMap(HashMap<String, Integer> columnsMap) {
        this.columnsMap = columnsMap;
    }

    public HashMap<String,String> getDateFormats() {
        return dateFormats;
    }

    public void setDateFormats(HashMap<String,String> dateFormats) {
        this.dateFormats = dateFormats;
    }
}
