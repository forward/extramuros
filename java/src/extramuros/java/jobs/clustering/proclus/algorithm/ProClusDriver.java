package extramuros.java.jobs.clustering.proclus.algorithm;

/**
 * User: antonio
 * Date: 23/05/2011
 * Time: 12:17
 */

import extramuros.java.jobs.clustering.proclus.Job;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProClusDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(ProClusDriver.class);

  private Configuration config;
  private FileSystem fs;

  private Path input;
  private Path outputDir;
  private int maxIterations;
  private double minDeviation;
  private float splitSize;
  private int kValue;
  private int lValue;
  private double minThreshold;

  public ProClusDriver() throws IOException {
    config = new Configuration();
    if(getConf()==null) {
        setConf(config);
    }
    fs = FileSystem.get(config);

  }

   public static void run(Configuration conf, Path input, Path output, int maxIterations, double minDeviation,
                   float splitSize, int kValue, int lValue, double minThreshold) throws IOException {

       ProClusDriver proclus = new ProClusDriver();
       proclus.buildClusters(conf,input,output, maxIterations, minDeviation, splitSize, kValue, lValue, minThreshold);
   }

   public void buildClusters(Configuration conf, Path input, Path output, int maxIterations, double minDeviation,
                   float splitSize, int kValue, int lValue, double minThreshold) {
       try {
           setConf(conf);
           this.fs = FileSystem.get(conf);
           this.input = input;
           outputDir  = output;
           this.maxIterations = maxIterations;
           this.minDeviation = minDeviation;
           this.splitSize = splitSize;
           this.kValue = kValue;
           this.lValue = lValue;
           this.minThreshold = minThreshold;

           runAlgorithm();
       } catch(Exception e) {
           log.error("Error running ProClus algorithm",e);
       }
   }

   public void buildClusters(Path input, Path output, int maxIterations, double minDeviation, float splitSize, int kValue,
                   int lValue, double minThreshold) {
       this.input = input;
       outputDir  = output;
       this.maxIterations = maxIterations;
       this.minDeviation = minDeviation;
       this.splitSize = splitSize;
       this.kValue = kValue;
       this.lValue = lValue;
       this.minThreshold = minThreshold;

       try {
           runAlgorithm();
       } catch(Exception e) {
           log.error("Error running ProClus algorithm",e);
       }
   }

   public boolean parseArgs(String[] args) throws Exception {
       DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
       ArgumentBuilder abuilder = new ArgumentBuilder();
       GroupBuilder gbuilder = new GroupBuilder();
       Option helpOpt = DefaultOptionCreator.helpOption();

       Option inputFileOpt = obuilder.withLongName("input").withRequired(true).withArgument(
               abuilder.withName("input").withMinimum(1).withMaximum(1).create()).withDescription(
               "The input file with the vectors of data to cluster").withShortName("i").create();

       Option outputDirOpt = obuilder.withLongName("output").withRequired(true).withArgument(
               abuilder.withName("output").withMinimum(1).withMaximum(1).create()).withDescription(
               "The data output directory").withShortName("o").create();

       Option maxIterationsOpt = obuilder.withLongName("maxIterations").withRequired(true).withArgument(
               abuilder.withName("iteration").withMinimum(1).withMaximum(1).create()).withDescription(
               "number of iteration the iterative step of the algorithm will run").withShortName("it").create();

       Option minThresholdOpt = obuilder.withLongName("minThreshold").withRequired(true).withArgument(
               abuilder.withName("minThreshold").withMinimum(1).withMaximum(1).create()).withDescription(
               "minimum optimization threshold").withShortName("th").create();

        Option splitSizeOpt = obuilder.withLongName("splitSize").withRequired(false).withArgument(
               abuilder.withName("splitSize").withMinimum(0).withMaximum(1).create()).withDescription(
               "% of the input data used to bootstrap the algorithm computing initial medoids [0,1]").withShortName("ss").create();

       Option minDeviationOpt = obuilder.withLongName("minDeviation").withRequired(false).withArgument(
               abuilder.withName("minDeviation").withMinimum(0).withMaximum(1).create()).withDescription(
               "minimum deviation in the evaluation of clusters [0,1]").withShortName("md").create();

       Option kValueOpt = obuilder.withLongName("k").withRequired(true).withArgument(
               abuilder.withName("k").withMinimum(1).withMaximum(1).create()).withDescription(
               "K number of initial medoids to foun (k>num clusters)").withShortName("k").create();

       Option lOpt = obuilder.withLongName("l").withRequired(true).withArgument(
               abuilder.withName("l").withMinimum(1).withMaximum(1).create()).withDescription(
               "Num of dimensions scale factor").withShortName("l").create();


       Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
               withOption(maxIterationsOpt).withOption(minDeviationOpt).withOption(kValueOpt).withOption(splitSizeOpt).
               withOption(lOpt).create();

       try {

           Parser parser = new Parser();
           parser.setGroup(group);
           CommandLine cmdLine = parser.parse(args);

           if (cmdLine.hasOption(helpOpt)) {
               CommandLineUtil.printHelp(group);
               return false;
           }


           input = new Path((String) cmdLine.getValue(inputFileOpt));
           maxIterations = Integer.parseInt(((String) cmdLine.getValue(maxIterationsOpt)));
           outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
           splitSize = Float.parseFloat((String) cmdLine.getValue(splitSizeOpt,"0.2"));
           kValue = Integer.parseInt((String) cmdLine.getValue(kValueOpt));
           lValue = Integer.parseInt((String) cmdLine.getValue(lOpt));
           minDeviation = Double.parseDouble((String) cmdLine.getValue(minDeviationOpt, "0.1"));
           minThreshold = Double.parseDouble((String) cmdLine.getValue(minThresholdOpt, "0"));



        } catch (OptionException e) {
            log.error("Command-line option Exception", e);
            CommandLineUtil.printHelp(group);
            return false;
        }

        return true;
      }

    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {

            return runAlgorithm();

        } else {
            throw new Exception("Impossible to parse arguments");
        }
    }

    protected int runAlgorithm() throws Exception {

           log.info("Recreating output dir : "+outputDir.toUri().getPath());

           if(fs.exists(outputDir)) {
               fs.delete(outputDir,true);
               fs.mkdirs(outputDir);
           }

            // evaluation metric
            double bestObjective = 0;

            // current set of clusters
            ClusterSet mBest = null;
            Path mBestPath = null;

            // Sampling input data
            ProClusSampler sampler = new ProClusSampler();
            sampler.run(getConf(), input,outputDir.suffix("/initialClusters"),splitSize);
            Path sampledDataPath = sampler.getOutput();


            // Running greedy bootstrap algorithm
            ProClusGreedyInitializationJob greedyInitializationJob = new ProClusGreedyInitializationJob();
            greedyInitializationJob.run(getConf(),sampledDataPath,outputDir,kValue);
            Path initialMedoidsFile = greedyInitializationJob.getOutputFile();


            Path iterationMedoidsFile = initialMedoidsFile;


            // Iterative phase
            for(int i=0; i<maxIterations; i++) {

                log.info("***** ITERATION "+i+"\n\n\n");
                // select dimensions for the current medoids
                ProClusDimensionSelectionJob dimensionSelectionJob = new ProClusDimensionSelectionJob();
                dimensionSelectionJob.run(getConf(),input,iterationMedoidsFile,outputDir,lValue);
                Path clustersPath = dimensionSelectionJob.getOutputFile();


                // assignation of points to the clusters
                ProClusPointsAssignmentJob pointsAssignmentJob = new ProClusPointsAssignmentJob();
                pointsAssignmentJob.run(getConf(),input,clustersPath,outputDir,i);
                clustersPath = pointsAssignmentJob.getOutputFile();


                // evaluation of clusters
                ProClusClusterEvaluationJob clusterEvaluationJob = new ProClusClusterEvaluationJob();
                clusterEvaluationJob.run(getConf(), input, clustersPath, outputDir, minDeviation, i);

                ClusterSet mCurrent = clusterEvaluationJob.getClusterSet();
                double objectiveFunction = clusterEvaluationJob.getEvaluationMetric();

                // Check if we must add random medoids
                log.info("OBJECTIVE:"+objectiveFunction+" VS "+bestObjective);
                boolean badMedoids = false;
                if(i == 0) {
                    mBest = mCurrent;
                    mBestPath = clustersPath;
                    bestObjective = objectiveFunction;
                } else if(objectiveFunction < bestObjective) {
                    mBest = mCurrent;
                    mBestPath = clustersPath;
                    bestObjective = objectiveFunction;
                    badMedoids = true;
                }

                if(bestObjective < minThreshold) {
                    log.info("MIN THRESHOLD BEATEN BY OBJECTIVE FUNCTION");
                    break;
                }

                // create next iteration input medoids
                if(i<(maxIterations-1)) {
                    iterationMedoidsFile = nextIterationMedoids(mBest,i, sampler, badMedoids);
                }

            }

            // Refinement phase

            // first we cluster points
            ProClusPointsClusteringJob clusteringJob = new ProClusPointsClusteringJob();
            clusteringJob.run(getConf(),input,mBestPath,outputDir);

            // we compute final clusters
            ProClusRefinementJob refinementJob = new ProClusRefinementJob();
            refinementJob.run(getConf(),clusteringJob.getOutput(),mBestPath, outputDir, lValue);

            // assignation of points to the clusters
            ProClusPointsAssignmentJob pointsAssignmentJob = new ProClusPointsAssignmentJob();
            pointsAssignmentJob.run(getConf(), input,refinementJob.getOutputFile(),outputDir,maxIterations);
            Path clustersPath = pointsAssignmentJob.getOutputFile();

            // evaluation of  final clusters
            ProClusClusterEvaluationJob clusterEvaluationJob = new ProClusClusterEvaluationJob();
            clusterEvaluationJob.run(getConf(), input, clustersPath, outputDir, minDeviation, maxIterations);

            ClusterSet mCurrent = clusterEvaluationJob.getClusterSet();
            double objectiveFunction = clusterEvaluationJob.getEvaluationMetric();

            // final cluster assignation
            clusteringJob = new ProClusPointsClusteringJob();
            clusteringJob.run(getConf(),input,clustersPath,outputDir);

            log.info("*** FINAL CLUSTERS ("+objectiveFunction+")");
            for(Cluster cluster : mCurrent.getClusters()) {
                log.info(cluster.getLabel()+ " : "+cluster.getNumPoints()+ " points clustered -> "+cluster.getCentroid());
                for(double dimension : cluster.getMedoid().getDimensions()){
                    log.info("  - "+dimension);
                }
            }

            return 0;
    }

    private Path nextIterationMedoids(ClusterSet mBest, int i, ProClusSampler sampler, boolean badMedoids) throws IOException, InstantiationException, IllegalAccessException {

        // draw enough vectors to avoiding duplicates
        int badMedoidsCount = mBest.count() - mBest.bestClustersCount();
        int toDraw = mBest.count() + badMedoidsCount;

        VectorWritable[] vectors = (VectorWritable[]) sampler.drawRandomVectors(toDraw);

        mBest.removeNotInBestClusters();
        for(VectorWritable writable : vectors) {
            Vector vector = writable.get();

            if(!mBest.vectorInMedoids(vector) && badMedoidsCount>0) {
                mBest.addCluster(new Cluster(new Medoid(vector)));
                badMedoidsCount--;
            }
        }

        MedoidSet set = new MedoidSet();
        for(Cluster cluster : mBest.getClusters()) {
            set.addMedoid(cluster.getMedoid());
        }

        set.computeDeltas();

        if(!fs.exists(outputDir.suffix("/medoids"))) {
            fs.mkdirs(outputDir.suffix("/medoids"));
        }
        Path outputPath = outputDir.suffix("/medoids/iteration-"+i+".txt");
        if(fs.exists(outputPath)) {
            fs.delete(outputPath,false);
        }

        SequenceFile.Writer outWriter = new SequenceFile.Writer(fs,config,outputPath,IntWritable.class,MedoidSet.class);
        try {
            outWriter.append(new IntWritable(set.count()), set);
        } finally {
            IOUtils.quietClose(outWriter);
        }

        return outputPath;
    }


    public static void main(String[] args) {
        try {
            ProClusDriver job = new ProClusDriver();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running ProClus driver job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
