package extramuros.java.jobs.utils;


import extramuros.java.jobs.clustering.proclus.algorithm.ClusterSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

class ClusterIterator implements Iterator<Cluster> {

    protected  Iterator<Pair<Writable,Writable>> reader;
    protected Path[] inputFiles;
    protected Cluster lastCluster;
    protected int currentFile;
    protected FileSystem fs;
    protected int counter;
    protected Configuration configuration;

    private static final Logger log = LoggerFactory.getLogger(ClusterIterator.class);

    ClusterIterator(Path input, Configuration configuration) throws IOException {
        this.lastCluster = null;

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
        if(lastCluster != null) {
            return true;
        } else {
            lastCluster = readNextCluster();
            return lastCluster != null;
        }
    }

    private Cluster readNextCluster() {
        try {
            if(reader.hasNext()) {
                Pair<Writable, Writable> pair = reader.next();
                Cluster secondComponent = (Cluster) pair.getSecond();
                return secondComponent;
            } else {
                currentFile++;
                if(currentFile < inputFiles.length) {
                    reader = TableUtils.fileSeqIterator(inputFiles[currentFile],configuration);
                    return readNextCluster();
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

    public Cluster next() {
        if(lastCluster!=null){
            Cluster tmp = lastCluster;
            lastCluster = null;
            return tmp;
        } else {
            return readNextCluster();
        }
    }

    public void remove() {
    }
}


class ClusteredPointsIterator implements Iterator<Pair<String,Vector>> {

    protected  Iterator<Pair<Writable,Writable>> reader;
    protected Path[] inputFiles;
    protected Pair<String,Vector> lastPair;
    protected int currentFile;
    protected FileSystem fs;
    protected int counter;
    protected Configuration configuration;

    private static final Logger log = LoggerFactory.getLogger(ClusterIterator.class);

    ClusteredPointsIterator(Path input, Configuration configuration) throws IOException {
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

    private Pair<String,Vector> readNextPair() {
        try {
            if(reader.hasNext()) {
                Pair<Writable, Writable> pair = reader.next();
                Writable secondComponent = pair.getSecond();
                if(secondComponent instanceof VectorWritable) {
                    Pair<String,Vector> tmp = new Pair<String, Vector>(
                            pair.getFirst().toString(),
                            ((VectorWritable) pair.getSecond()).get()
                    );
                    return tmp;
                } else {
                    Pair<String,Vector> tmp = new Pair<String, Vector>(
                            pair.getFirst().toString(),
                            ((WeightedVectorWritable) pair.getSecond()).getVector()
                    );
                    return tmp;
                }

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

    public Pair<String,Vector> next() {
        if(lastPair!=null){
            Pair<String,Vector> tmp = lastPair;
            lastPair = null;
            return tmp;
        } else {
            return readNextPair();
        }
    }

    public void remove() {
    }
}

/**
 * User: antonio
 * Date: 07/06/2011
 * Time: 09:32
 */
public class ClusterUtils {
    public static Iterator<Cluster> clusterIterator(Path clustersDirectory, Configuration configuration) {
        try {
            Iterator<Pair<Writable, Writable>> iterator = TableUtils.directorySeqIterator(clustersDirectory, configuration);
            if(iterator.hasNext()) {
                Writable secondComp = iterator.next().getSecond();
                if(secondComp instanceof ClusterSet) {
                    return ((ClusterSet)secondComp).iterator();
                } else {
                    return new ClusterIterator(clustersDirectory,configuration);
                }
            } else {
                return new ClusterIterator(clustersDirectory,configuration);
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static Iterator<Pair<String,Vector>> clusteredPointsIterator(Path pointsDirectory, Configuration configuration) {
        try {
         return new ClusteredPointsIterator(pointsDirectory, configuration);
        } catch (Exception e) {
            return null;
        }
    }

    public static Path lastClusterIteration(Path clustersDirectory, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] status = fs.listStatus(clustersDirectory);

        ArrayList<Path> paths = new ArrayList<Path>(status.length);

        for(int i=0; i<status.length; i++) {
            FileStatus currentStatus = status[i];
            if(Pattern.matches(".*clusters-[0-9]+.*", currentStatus.getPath().toUri().getPath().toLowerCase())) {
                paths.add(currentStatus.getPath());
            }
        }

        Collections.sort(paths,
                new Comparator<Path>() {

                    public int compare(Path path1, Path path2) {
                        String[] parts1 = path1.toUri().getPath().split("-");
                        String[] parts2 = path2.toUri().getPath().split("-");

                        int parts1Int = Integer.parseInt(parts1[parts1.length-1]);
                        int parts2Int = Integer.parseInt(parts2[parts2.length-1]);

                        return - (new Integer(parts1Int)).compareTo(new Integer(parts2Int));
                    }
                });

        return paths.get(0);
    }
}
