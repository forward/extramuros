package extramuros.java.jobs.clustering.validation.daviesbouldin;

import extramuros.java.jobs.utils.ClusterUtils;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * User: antonio
 * Date: 13/06/2011
 * Time: 09:33
 */
public class Job extends ExtramurosJob {

    protected Path clustersPath;
    protected Path outputPath;
    protected Path vectorsPath;
    protected double index;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String clustersPath, String vectorsPath, String outputPath, Configuration configuration) throws IOException {
        super(configuration);

        this.clustersPath = new Path(clustersPath);
        this.vectorsPath = new Path(vectorsPath);
        this.outputPath = new Path(outputPath);
    }

    @Override
    public Path getOutputFile() {
        return outputPath;
    }

    @Override
    public Object getOutput() {
        return index;
    }

    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, clustersPath.toUri().getPath());

            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "Davies-Bouldin Index validation job"));

            getJob().setJobName("davies_bouldin_job");

            getJob().setMapperClass(Mapper.class);
            getJob().setReducerClass(Reducer.class);

            getJob().setMapOutputKeyClass(Text.class);
            getJob().setMapOutputValueClass(DoubleWritable.class);

            getJob().setOutputKeyClass(Text.class);
            getJob().setOutputValueClass(DoubleWritable.class);

            getJob().setInputFormatClass(SequenceFileInputFormat.class);
            getJob().setOutputFormatClass(SequenceFileOutputFormat.class);

            String inputPathString = composeInputPathString(vectorsPath);
            FileInputFormat.addInputPaths(getJob(), inputPathString);
            FileOutputFormat.setOutputPath(getJob(),outputPath);

            getJob().setJarByClass(Job.class);

            if (!getJob().waitForCompletion(true)) {
                throw new InterruptedException("Davies-Bouldin Index job failed processing vectors" + vectorsPath.toUri().getPath() +
                        "clusters: " + clustersPath.toUri().getPath() + " output: " + outputPath.toUri().getPath());
            }

            cleanOutput(outputPath);

            computeIndex();

        } catch (IOException e) {
            log.error("Error running job", e);
        } catch (InterruptedException e) {
            log.error("Error running job", e);
        } catch (ClassNotFoundException e) {
            log.error("Error running job", e);
        }
    }

    private void computeIndex() throws IOException {

        HashMap<Integer, Cluster> clustersMap = new HashMap<Integer, Cluster>();
        HashMap<Integer, HashMap<Integer,Double>> interClusterDistances = new HashMap<Integer, HashMap<Integer, Double>>();
        EuclideanDistanceMeasure distance = new EuclideanDistanceMeasure();

        Iterator<Cluster> clustersIterator = ClusterUtils.clusterIterator(clustersPath, getConf());

        while(clustersIterator.hasNext()) {
            Cluster cluster = clustersIterator.next();
            clustersMap.put(cluster.getId(),cluster);
            interClusterDistances.put(cluster.getId(),new HashMap<Integer, Double>());
        }

        for(Cluster cluster : clustersMap.values()) {
            for(Cluster toCluster : clustersMap.values()) {
                if(cluster.getId() != toCluster.getId()) {
                    HashMap<Integer,Double> distancesMap = interClusterDistances.get(cluster.getId());
                    distancesMap.put(toCluster.getId(),distance.distance(cluster.getCenter(),toCluster.getCenter()));
                }
            }
        }

        Iterator<Pair<Writable, Writable>> iterator =  TableUtils.fileSeqIterator(outputPath,getConf());

        HashMap<Integer,Double> intraClusterDistances = new HashMap<Integer, Double>();
        while(iterator.hasNext()) {
            Pair<Writable,Writable> datum = iterator.next();
            int clusterId = Integer.parseInt(((Text)datum.getFirst()).toString());
            double avgDistance = ((DoubleWritable) datum.getSecond()).get();

            intraClusterDistances.put(clusterId,avgDistance);
        }

        double indexAcum = 0;

        for(Cluster cluster : clustersMap.values()) {
            double max = 0;
            boolean  initMax = false;
            for(Cluster toCluster : clustersMap.values()) {
                if(cluster.getId() != toCluster.getId()) {
                    if(!initMax) {
                        initMax = true;
                        max = (intraClusterDistances.get(cluster.getId()) + intraClusterDistances.get(toCluster.getId()))
                                /(interClusterDistances.get(cluster.getId()).get(toCluster.getId()));
                    } else {
                        double value = (intraClusterDistances.get(cluster.getId()) + intraClusterDistances.get(toCluster.getId()))
                                /(interClusterDistances.get(cluster.getId()).get(toCluster.getId()));
                        if(value > max) {
                            max = value;
                        }
                    }
                }
            }
            indexAcum = indexAcum + max;
        }

        index= indexAcum / (double) clustersMap.size();

        SequenceFile.Writer outWriter = null;
        try {
            outputPath = outputPath.suffix("/index.seq");
            outWriter = new SequenceFile.Writer(getFs(), getConf(), outputPath,
                    Text.class.asSubclass(Writable.class),
                    DoubleWritable.class.asSubclass(Writable.class));

            Writable key = new Text("Davies-Bouldin Index");
            Writable value = new DoubleWritable(index);

            outWriter.append(key, value);

        } catch (Exception e) {
            log.error("Error writing index output file at "+outputPath, e);
        } finally {
            if(outWriter!=null)
                IOUtils.quietClose(outWriter);
        }
    }


    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
