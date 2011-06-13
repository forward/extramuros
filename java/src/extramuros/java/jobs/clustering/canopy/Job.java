package extramuros.java.jobs.clustering.canopy;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
import extramuros.java.jobs.utils.ClusterUtils;
import extramuros.java.jobs.utils.ExtramurosJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * User: antonio
 * Date: 06/06/2011
 * Time: 16:10
 */
public class Job extends ExtramurosJob {

    private static final Logger log = LoggerFactory.getLogger(Job.class);

    protected Path outputDirectory;
    protected AbstractTable table;
    protected DistanceMeasure distance;
    protected double t1;
    protected double t2;
    protected Configuration configuration;
    protected boolean runClustering;

    public Job(String outputDirectory, AbstractTable table, DistanceMeasure distance, double t1, double t2,
               boolean runClustering, Configuration configuration) throws IOException {
        super(configuration);

        this.outputDirectory = new Path(outputDirectory);
        this.table = table;
        this.distance = distance;
        this.t1 = t1;
        this.t2 = t2;
        this.configuration = configuration;
        this.runClustering = runClustering;
    }

    @Override
    public void run() {
        try {
            if(getFs().exists(outputDirectory)) {
                getFs().delete(outputDirectory,true);
                getFs().mkdirs(outputDirectory);
            }

            Path inputPath = null;
            if (table.getClass() == VectorSeqTableAdapter.class) {
                inputPath = new Path(table.getRowsPath());
            } else {
                Path vectorizationPath = outputDirectory.suffix("/vectorization");
                ArrayList<String> columns = new ArrayList<String>(table.getHeader().getColumnNames().size());
                for (String column : table.getHeader().getColumnNames()) {
                    if (table.getHeader().typeFor(column) != RowTypes.CATEGORICAL && table.getHeader().typeFor(column) != RowTypes.STRING) {
                        columns.add(column);
                    }
                }

                String[] columnPaths = new String[columns.size()];
                for(int i=0; i<columnPaths.length; i++){
                    columnPaths[i] = columns.get(i);
                }
                extramuros.java.jobs.file.vectorize.Job vectorizeJob = new extramuros.java.jobs.file.vectorize.Job(
                        vectorizationPath.toUri().getPath(),
                        columnPaths,
                        DenseVector.class, table, configuration);

                vectorizeJob.run();

                // new table
                this.table = (AbstractTable) vectorizeJob.getOutput();
                table.setConfiguration(getConf());
                table.setTablePath(outputDirectory.suffix("/vectorization.tbl").toUri().getPath());
                table.save();

                // new output path
                outputDirectory = outputDirectory.suffix("/canopy");

                // input path
                inputPath = new Path(table.getRowsPath());
            }

            CanopyDriver.run(configuration,  inputPath,  outputDirectory,  distance, t1, t2, runClustering, false);

            cleanOutput(outputDirectory.suffix("/clusters-0"));
            if(runClustering) {
                cleanOutput(outputDirectory.suffix("/clusteredPoints"));
            }

        } catch (Exception e) {
            log.error("Error running canopy job adapter", e);
        }
    }

    @Override
    public Path getOutputFile() {
        return outputDirectory;
    }

    @Override
    public Object getOutput() {
        HashMap<String,Object> output = new HashMap<String, Object>();
        if(runClustering) {
            VectorSeqTableAdapter clusteredTable = new VectorSeqTableAdapter(table.getHeader(),outputDirectory.suffix("/clusteredPoints/").toUri().getPath());
            clusteredTable.setConfiguration(getConf());
            clusteredTable.setTablePath(outputDirectory.suffix("/clusteredPoints.tbl").toUri().getPath());
            clusteredTable.save();

            output.put("clustered-points",clusteredTable);
        }

        output.put("clusters",outputDirectory.suffix("/clusters-0").toUri().getPath());

        output.put("cluster-iterator", ClusterUtils.clusterIterator(
                new Path(outputDirectory.suffix("/clusters-0").toUri().getPath()),
                configuration));

        if(runClustering) {
            output.put("points-iterator", ClusterUtils.clusteredPointsIterator(
                    new Path(outputDirectory.suffix("/clusteredPoints").toUri().getPath()),
                    configuration
            ));
        }
        return output;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
