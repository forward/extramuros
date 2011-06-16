package extramuros.java.jobs.clustering.proclus.algorithm;

import extramuros.java.formats.TableHeader;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.TableUtils;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.jet.random.sampling.RandomSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.BitSet;
import java.util.Iterator;


/**
 * User: antonio
 * Date: 23/05/2011
 * Time: 12:26
 */
public class ProClusSampler {

    private Path input;
    private Path outputDir;
    private float splitSize;
    private Configuration config;
    private FileSystem fs;
    private int totalLines;

    private static final Logger log = LoggerFactory.getLogger(ProClusSampler.class);

    public ProClusSampler() throws IOException {
        config = new Configuration();
        fs = FileSystem.get(config);
    }


    /**
     * Parse the arguments for the job:
     *  - inputFile: file with the initial data points
     *  - outputDir: directory where the sample will be written
     *  - splitSize: size of the sample in percentage
     */
    public boolean parseArgs(String[] args) throws Exception {

      DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
      ArgumentBuilder abuilder = new ArgumentBuilder();
      GroupBuilder gbuilder = new GroupBuilder();
      Option helpOpt = DefaultOptionCreator.helpOption();

      Option inputFileOpt = obuilder.withLongName("input").withRequired(true).withArgument(
          abuilder.withName("input").withMinimum(1).withMaximum(1).create()).withDescription(
          "The input files").withShortName("i").create();

      Option outputDirOpt = obuilder.withLongName("outputDir").withRequired(true).withArgument(
          abuilder.withName("outputDir").withMinimum(1).withMaximum(1).create()).withDescription(
          "The data output directory").withShortName("o").create();


      Option splitSizeOpt = obuilder.withLongName("splitSize").withRequired(false).withArgument(
          abuilder.withName("splitSize").withMinimum(1).withMaximum(1).create()).withDescription(
          "The split size for the input data").withShortName("ss").create();


      Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt)
           .withOption(splitSizeOpt).create();

      try {

          Parser parser = new Parser();
          parser.setGroup(group);
          CommandLine cmdLine = parser.parse(args);

          if (cmdLine.hasOption(helpOpt)) {
              CommandLineUtil.printHelp(group);
              return false;
          }

          input = new Path((String) cmdLine.getValue(inputFileOpt));
          outputDir = new Path((String) cmdLine.getValue(outputDirOpt));
          splitSize = Float.parseFloat((String) cmdLine.getValue(splitSizeOpt));

      } catch (OptionException e) {
          log.error("Command-line option Exception", e);
          CommandLineUtil.printHelp(group);
          return false;
      }

      return true;
    }

    public void run() {
        try {

            config.set(ProClusConfigKeys.PROBABILITY, ""+splitSize);

            // prepare Hadoop job
            Job job = new Job(config, "Probabilistic sampling input data at: "+input.toUri().getPath()
                            + " with probability: " + splitSize);

            job.setJobName("probabilistic_sample_job");

            job.setMapperClass(ProClusSamplerMapper.class);
            job.setNumReduceTasks(0);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(VectorWritable.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(VectorWritable.class);


            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            String inputPathString = ProClusUtils.composeInputPathString(fs,input);
            log.info("Composed input path "+inputPathString);
            FileInputFormat.addInputPaths(job, inputPathString);
            FileOutputFormat.setOutputPath(job, outputDir);

            job.setJarByClass(Job.class);

            if (!job.waitForCompletion(true)) {
                throw new InterruptedException("Probabilistic sample job failed processing " + input.toUri().getPath()
                        + " output: " + outputDir.toUri().getPath());
            }

            ProClusUtils.cleanOutput(fs,outputDir);

        } catch (IOException e) {
            log.error("Error running job", e);
        } catch (InterruptedException e) {
            log.error("Error running job", e);
        } catch (ClassNotFoundException e) {
            log.error("Error running job", e);
        }
    }


    public Path getOutput() {
        return outputDir;
    }

    public void run(Configuration config, Path inputFile, Path outputDir, float splitSize) throws Exception, InstantiationException {
        this.config = config;
        this.fs = FileSystem.get(config);
        this.input = inputFile;
        this.outputDir = outputDir;
        this.splitSize = splitSize;

        run();
    }



    public int countLines() throws IOException {
        VectorSeqTableAdapter table = new VectorSeqTableAdapter(new TableHeader(),input.toUri().getPath());

        extramuros.java.jobs.file.countlines.Job job = new extramuros.java.jobs.file.countlines.Job(outputDir.suffix("/countLines"),table,config);

        job.run();

        return (Integer) job.getOutput();
    }

    public VectorWritable[] drawRandomVectors(int toDraw) throws IOException, IllegalAccessException, InstantiationException {

        totalLines = countLines();

        log.debug("TOTAL LINES:"+totalLines);
        log.debug("LINES to DRAW:"+toDraw);

        //SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,config);

        VectorWritable[] toReturn = new VectorWritable[toDraw];
        long[] selectedLines = new long[toDraw];

        log.debug("DRAWING "+toDraw+" from "+totalLines);
        RandomSampler.sample(toDraw, totalLines - 1, toDraw, 0, selectedLines, 0, RandomUtils.getRandom());

        BitSet randomSel = new BitSet(totalLines);
        for (long selectedLine : selectedLines) {
            randomSel.set((int) selectedLine + 1);
        }

        int pos = 0;
        int count = 0;

        Iterator<org.apache.mahout.common.Pair<Writable,Writable>> iterator = TableUtils.directorySeqIterator(input, config);
        while(iterator.hasNext()) {
            VectorWritable value = (VectorWritable) iterator.next().getSecond();
            pos++;
            if(randomSel.get(pos)) {
                toReturn[count] = value;
                count++;
            }
        }
            /*
            Writable key = (Writable) reader.getKeyClass().newInstance();
            VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                pos++;
                if(randomSel.get(pos)){
                    toReturn[count] = value;
                    count++;
                }
            }
            */
        log.info("Drawn "+count+"/"+pos+" lines.");

        return toReturn;
    }


    public static void main(String[] args) {
        try {
            ProClusSampler sampler = new ProClusSampler();
            sampler.parseArgs(args);
            sampler.run();
        } catch (Exception ex) {
            log.error("Error running ProClusSampler",ex);
        }
    }

}

