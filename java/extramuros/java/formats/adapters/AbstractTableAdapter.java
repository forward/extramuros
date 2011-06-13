package extramuros.java.formats.adapters;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.TableHeader;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 09:07
 */
public abstract class AbstractTableAdapter<L,P> extends Mapper<Writable, L, Writable, Row> implements SkipFilter<L>, SplitFunction<L,P>, NullDetector<P>{

    protected TableHeader tableHeader;

    protected static final Logger log = LoggerFactory.getLogger(Mapper.class);


    public abstract Row parse(int id, P[] parts);


    public Row parseLine(int id, L line) {
        P[] parts = split(line);
         for(int i=0; i<parts.length; i++) {
             if(isNull(parts[i])) {
                 parts[i] = null;
             }
         }
        return parse(id, parts);
    }


    public void setTableHeader(TableHeader tableHeader) {
        this.tableHeader = tableHeader;
    };

    public abstract Class<? extends InputFormat> inputFormat();

    public abstract Row map(Writable key, L value);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading table from path: "+tablePath.toUri().getPath());
        try {
            //Table table = TableUtils.readTable(tablePath,config);
            AbstractTable table = TableUtils.readAbstractTable(tablePath,config);
            this.tableHeader = table.getHeader();
        } catch (Exception e) {
            log.error("Error reading Table in mapper ",e);
        }
    }
}
