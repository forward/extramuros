package extramuros.java.jobs.file.sample;

import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.jet.random.sampling.RandomSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;


/**
 * User: antonio
 * Date: 02/06/2011
 * Time: 12:45
 */
public class Job extends ExtramurosJob {

    protected Path fileToSample;
    protected Path outputFilePath;
    protected int totalLines;
    protected float percentageToSample;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String fileToSample, String outputFilePath, int totalLines, float percentageToSample, Configuration configuration) throws IOException {
        this(new Path(fileToSample), new Path(outputFilePath), totalLines, percentageToSample, configuration);
    }

    public Job(Path fileToSample, Path outputFilePath, int totalLines, float percentageToSample, Configuration configuration) throws IOException {
        super(configuration);

        this.fileToSample = fileToSample;
        this.outputFilePath = outputFilePath;
        this.totalLines = totalLines;
        this.percentageToSample = percentageToSample;
    }

    @Override
    public void run() {
        int linesInSplitSize = Math.round(totalLines * percentageToSample);
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(getFs(), fileToSample, getConf());
            SequenceFile.Writer outWriter = new SequenceFile.Writer(getFs(), getConf(), outputFilePath,
                    reader.getKeyClass().asSubclass(Writable.class),
                    reader.getValueClass().asSubclass(Writable.class));

            long[] selectedLines = new long[linesInSplitSize];

            RandomSampler.sample(linesInSplitSize, totalLines - 1, linesInSplitSize, 0, selectedLines, 0, RandomUtils.getRandom());

            BitSet randomSel = new BitSet(totalLines);
            for (long selectedLine : selectedLines) {
                randomSel.set((int) selectedLine + 1);
            }

            int pos = 0;
            int count = 0;

            try {
                Writable key = (Writable) reader.getKeyClass().newInstance();
                Writable value = (Writable) reader.getValueClass().newInstance();

                while (reader.next(key, value)) {
                    pos++;
                    if (randomSel.get(pos)) {
                        count++;
                        outWriter.append(key, value);
                    }
                }
                log.info("SAMPLED " + count + "/" + pos + " LINES.");
            } finally {
                IOUtils.quietClose(reader);
                IOUtils.quietClose(outWriter);
            }
        } catch (Exception e) {
            log.error("Error sampling file " + fileToSample.toUri().getPath(), e);
        }
    }

    @Override
    public Path getOutputFile() {
        return outputFilePath;
    }

    @Override
    public Object getOutput() {
        try {
            return TableUtils.fileSeqIterator(outputFilePath,getConf());
        } catch (IOException e) {
            log.error("Error reading output file "+outputFilePath.toUri().getPath(),e);
            return null;
        }
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}

