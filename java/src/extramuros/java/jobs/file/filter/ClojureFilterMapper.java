package extramuros.java.jobs.file.filter;

import extramuros.java.formats.Row;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 23/06/2011
 * Time: 12:51
 */
public class ClojureFilterMapper extends AbstractFilterMapper {

    private static final Logger log = LoggerFactory.getLogger(ClojureFilterMapper.class);

    @Override
    protected boolean filter(Row row) {
        //String clojureFilterfunction = (String) filterInformation;

        try {
            return(Boolean) TableUtils.applyDefinedFunctionToRow("extramuros_java_jobs_file_filterFunction", table, row);
            //return (Boolean) TableUtils.applyClojuFunctionToRow(clojureFilterfunction, table, row);

        } catch (Exception e) {
            log.error("Error applying Clojure function in mapper.", e);
            return false;
        }
    }

    @Override
    protected void customSetup(Context context) throws IOException, InterruptedException {
        TableUtils.defineFunction("extramuros_java_jobs_file_filterFunction", (String) this.filterInformation);
    }
}
