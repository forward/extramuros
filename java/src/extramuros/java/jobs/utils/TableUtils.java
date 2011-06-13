package extramuros.java.jobs.utils;

import extramuros.java.formats.AbstractTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:05
 */

class SeqFileIterator implements Iterator<Pair<Writable,Writable>> {

    protected SequenceFile.Reader reader;
    protected Pair<Writable,Writable> lastPair;

    private static final Logger log = LoggerFactory.getLogger(SeqFileIterator.class);

    SeqFileIterator(SequenceFile.Reader reader) {
        this.reader = reader;
        this.lastPair = null;
    }

    public boolean hasNext() {
        if(lastPair != null) {
            return true;
        } else {
            lastPair = readNextPair();
            return lastPair != null;
        }
    }

    private Pair<Writable,Writable> readNextPair() {
        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();

            if(reader.next(key,value)) {
                return new Pair<Writable, Writable>(key,value);
            } else {
                org.apache.mahout.common.IOUtils.quietClose(reader);
                return null;
            }
        } catch (InstantiationException e) {
            log.error(e.getMessage());
            log.error("InstantationException ERROR READING PAIR",e);
            return null;
        } catch (IllegalAccessException e) {
            log.error(e.getMessage());
            log.error("IllegalAccessException ERROR READING PAIR",e);
            return null;
        } catch (IOException e) {
            log.error(e.getMessage());
            log.error("IOException ERROR READING PAIR",e);
            return null;
        }
    }

    public Pair<Writable,Writable> next() {
        if(lastPair!=null){
            Pair<Writable,Writable> tmp = lastPair;
            lastPair = null;
            return tmp;
        } else {
            return readNextPair();
        }
    }

    public void remove() {
    }
}

class SeqDirectoryIterator implements Iterator<Pair<Writable,Writable>> {

    protected  Iterator<Pair<Writable,Writable>> reader;
    protected Path[] inputFiles;
    protected Pair<Writable,Writable> lastPair;
    protected int currentFile;
    protected FileSystem fs;
    protected int counter;
    protected Configuration configuration;

    private static final Logger log = LoggerFactory.getLogger(ClusterIterator.class);

    SeqDirectoryIterator(Path input, Configuration configuration) throws IOException {
        this.lastPair = null;

        // Check files
        this.configuration = configuration;
        this.fs = FileSystem.get(configuration);
        if(fs.isFile(input)) {
            inputFiles = new Path[1];
            inputFiles[0] = input;
        } else {
            FileStatus[] children = fs.listStatus(input);
            ArrayList<Path> childrenPaths = new ArrayList<Path>(children.length);
            for(FileStatus child : children) {
                if(fs.isFile(child.getPath())) {
                    childrenPaths.add(child.getPath());
                }
            }

            inputFiles = childrenPaths.toArray(new Path[childrenPaths.size()]);
        }

        currentFile = 0;
        counter = -1;
        reader = TableUtils.fileSeqIterator(inputFiles[currentFile],configuration);
    }

    public boolean hasNext() {
        if(lastPair != null) {
            return true;
        } else {
            lastPair = readNextPair();
            return lastPair != null;
        }
    }

    private Pair<Writable,Writable> readNextPair() {
        try {
            if(reader.hasNext()) {
                Pair<Writable, Writable> pair = reader.next();
                Writable secondComponent = pair.getSecond();
                return pair;
            } else {
                currentFile++;
                if(currentFile < inputFiles.length) {
                    reader = TableUtils.fileSeqIterator(inputFiles[currentFile],configuration);
                    return readNextPair();
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            log.error("IOException ERROR READING CLUSTER",e);
            return null;
        }
    }

    public Pair<Writable,Writable> next() {
        if(lastPair!=null){
            Pair<Writable,Writable> tmp = lastPair;
            lastPair = null;
            return tmp;
        } else {
            return readNextPair();
        }
    }

    public void remove() {
    }
}

public class TableUtils {

    public static AbstractTable readAbstractTable(Path input, Configuration config) throws IOException, IllegalAccessException, InstantiationException {

        FileSystem fs = FileSystem.get(config);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,config);

        Writable value = null;
        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            value = (Writable) reader.getValueClass().newInstance();
            reader.next(key,value);

        } finally {
            org.apache.mahout.common.IOUtils.quietClose(reader);
        }

        return (AbstractTable) value;
    }

    public static Writable[] readFirstWritable(Path input, Configuration config) throws IOException, IllegalAccessException, InstantiationException {

        FileSystem fs = FileSystem.get(config);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,config);
        Writable[] output = new Writable[2];


        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();

            reader.next(key,value);

            output[0] = key;
            output[1] = value;

        } finally {
            org.apache.mahout.common.IOUtils.quietClose(reader);
        }

        return output;
    }

    public static void writeSingleWritable(Path outputFile, Writable key, Writable value, Configuration configuration) throws IOException {
        SequenceFile.Writer outWriter = null;
        try {
            outWriter = new SequenceFile.Writer(FileSystem.get(configuration), configuration, outputFile,key.getClass(),value.getClass());
            outWriter.append(key, value);
        } finally {
            if(outWriter!=null)
                IOUtils.quietClose(outWriter);
        }
    }

    public static Iterator<Pair<Writable,Writable>> fileSeqIterator(Path input, Configuration config) throws IOException {
        FileSystem fs = FileSystem.get(config);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,config);


        Iterator<Pair<Writable,Writable>> it = new SeqFileIterator(reader);
        return it;
    }

    public static Iterator<Pair<Writable, Writable>> directorySeqIterator(Path inputDirectory, Configuration config) throws  IOException {
        return new SeqDirectoryIterator(inputDirectory,config);
    }

    public static int randomId() {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        uuid = uuid.replace("a", "11").replace("b","12").replace("c","13");
        uuid = uuid.replace("d","14").replace("e","15").replace("f","16");

        return Integer.parseInt(uuid);
    }

    public static Integer parseWritableKeyId(Writable key) {
        Integer id = null;

        if(key instanceof LongWritable) {
            id = new Long(((LongWritable)key).get()).intValue();
        } else if(key instanceof DoubleWritable) {
            id = new Double(((DoubleWritable)key).get()).intValue();
        } else if(key instanceof IntWritable) {
            id = ((IntWritable) key).get();
        } else if(key instanceof FloatWritable) {
            id = new Float(((FloatWritable)key).get()).intValue();
        }

        return id;
    }

}
