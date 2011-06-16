package extramuros.java.jobs.clustering.proclus;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
import extramuros.java.jobs.clustering.proclus.algorithm.ClusterSet;
import extramuros.java.jobs.clustering.proclus.algorithm.ProClusDriver;
import extramuros.java.jobs.utils.ClusterUtils;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
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
    protected int maxIterations;
    protected double minDeviation;
    protected double minThreshold;
    protected float splitSize;
    protected int kValue;
    protected int lValue;

    protected Configuration configuration;
    protected boolean runClustering;

    public Job(AbstractTable table, String outputdirectory, int maxIterations, double minDeviation, float splitSize, int kValue, int lValue,
               double minThreshold, Configuration configuration) throws IOException {
        super(configuration);

        this.outputDirectory = new Path(outputdirectory);
        this.table = table;
        this.configuration = configuration;
        this.maxIterations = maxIterations;
        this.minDeviation = minDeviation;
        this.minThreshold = minThreshold;
        this.splitSize = splitSize;
        this.kValue = kValue;
        this.lValue = lValue;
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
                outputDirectory = outputDirectory.suffix("/proclus");

                // input path
                inputPath = new Path(table.getRowsPath());
            }

            ProClusDriver.run(configuration,inputPath, outputDirectory, maxIterations, minDeviation, splitSize, kValue, lValue, minThreshold);

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

        VectorSeqTableAdapter clusteredTable = new VectorSeqTableAdapter(table.getHeader(),outputDirectory.suffix("/clusteredPoints/").toUri().getPath());
        clusteredTable.setConfiguration(getConf());
        clusteredTable.setTablePath(outputDirectory.suffix("/clusteredPoints.tbl").toUri().getPath());
        clusteredTable.save();

        output.put("clustered-points",clusteredTable);

        output.put("clusters",outputDirectory.suffix("/result/clusters.txt").toUri().getPath());


        try {
            Writable[] results = TableUtils.readFirstWritable(outputDirectory.suffix("/result/clusters.txt"), getConf());
            ClusterSet clusterSet = (ClusterSet) results[1];
            output.put("cluster-iterator", clusterSet.iterator());
        } catch (Exception e) {
            log.error("Error retrieving cluster iterator for computed output",e);
            e.printStackTrace();
        }


        output.put("points-iterator", ClusterUtils.clusteredPointsIterator(
                new Path(outputDirectory.suffix("/clusteredPoints").toUri().getPath()),
                configuration
        ));

        return output;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
