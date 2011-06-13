package extramuros.java.formats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 08:50
 */
public interface AbstractTable extends Writable, Iterable<Row> {
    public void setConfiguration(Configuration config);
    public Configuration getConfiguration();
    public TableHeader getHeader();
    public void setHeader(TableHeader header);
    public String getRowsPath();
    public void setRowsPath(String rowsPath);
    public String getTablePath();
    public void setTablePath(String tablePath);
    public boolean isAdapter();
    public AbstractTable clone();
    public boolean save();
}
