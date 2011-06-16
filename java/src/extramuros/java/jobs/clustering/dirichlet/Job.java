package extramuros.java.jobs.clustering.dirichlet;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
import extramuros.java.jobs.utils.ClusterUtils;
import extramuros.java.jobs.utils.ExtramurosJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.dirichlet.DirichletDriver;
import org.apache.mahout.clustering.dirichlet.models.DistributionDescription;

import org.apache.mahout.math.Vector;
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
    protected int numModels;
    protected int maxIterations;
    protected double alpha0;
    protected boolean emitMostLikely;
    protected double threshold;
    protected Configuration configuration;
    protected boolean runClustering;
    protected String modelDistributionClassName;
    protected String vectorModelClassName;
    protected String distanceMeasureClassName;

    //
    public Job(String outputDirectory, AbstractTable table, String modelDistributionClassName, String vectorModelClassName,
               String distanceMeasureClassName, int numModels, int maxIterations, double alpha0, boolean emitMostLikely,
               double threshold, boolean runClustering, Configuration configuration) throws Exception {
        super(configuration);

        this.outputDirectory = new Path(outputDirectory);
        this.table = table;
        this.numModels = numModels;
        this.maxIterations = maxIterations;
        this.emitMostLikely = emitMostLikely;
        this.alpha0 = alpha0;
        this.threshold = threshold;
        this.configuration = configuration;
        this.runClustering = runClustering;

        this.modelDistributionClassName = modelDistributionClassName;
        this.vectorModelClassName = vectorModelClassName;
        this.distanceMeasureClassName = distanceMeasureClassName;
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

                Class<? extends Vector>  vectorKlass = Class.forName(vectorModelClassName).asSubclass(Vector.class);

                extramuros.java.jobs.file.vectorize.Job vectorizeJob = new extramuros.java.jobs.file.vectorize.Job(
                        vectorizationPath.toUri().getPath(),
                        columnPaths,
                        vectorKlass, table, configuration);

                vectorizeJob.run();

                // new table
                this.table = (AbstractTable) vectorizeJob.getOutput();
                table.setConfiguration(getConf());
                table.setTablePath(outputDirectory.suffix("/vectorization.tbl").toUri().getPath());
                table.save();

                // new output path
                outputDirectory = outputDirectory.suffix("/dirichlet");

                // input path
                inputPath = new Path(table.getRowsPath());
            }

            DistributionDescription distributionDescription = new DistributionDescription(modelDistributionClassName,
                    vectorModelClassName,distanceMeasureClassName, table.getHeader().getColumnNames().size());
            DirichletDriver.run(getConf(),new Path(table.getRowsPath()),outputDirectory,distributionDescription,numModels,maxIterations,
                    alpha0,runClustering,emitMostLikely,threshold,false);

            cleanOutput(ClusterUtils.lastClusterIteration(outputDirectory,getConf()));
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

        try {
            output.put("clusters",ClusterUtils.lastClusterIteration(outputDirectory, getConf()).toUri().getPath());

            output.put("cluster-iterator", ClusterUtils.clusterIterator(
                ClusterUtils.lastClusterIteration(outputDirectory,getConf()),
                configuration));
        } catch (IOException e) {
            output.put("clusters",outputDirectory.toUri().getPath());
        }



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
