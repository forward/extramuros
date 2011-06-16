package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

class ProClusClustersIterator implements Iterator<org.apache.mahout.clustering.Cluster> {
    ClusterSet clusterSet;
    Cluster currentCluster;
    int counter = 0;

    public ProClusClustersIterator(ClusterSet clusterSet) {
        this.clusterSet = clusterSet;
        if(clusterSet.clusters.size()>0) {
            currentCluster = clusterSet.clusters.get(0);
        } else {
            currentCluster = null;
        }
    }

    public boolean hasNext() {
        return currentCluster != null;
    }

    public Cluster next() {
        Cluster tmp = currentCluster;
        counter++;
        if(counter < clusterSet.clusters.size()) {
            currentCluster = clusterSet.clusters.get(counter);
        } else {
            currentCluster = null;
        }

        return tmp;
    }

    public void remove() {

    }
}
/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 13:24
 */
public class ClusterSet implements Writable, WritableComparable<ClusterSet>, Iterable<org.apache.mahout.clustering.Cluster> {

    ArrayList<Cluster> clusters;

    public ClusterSet() {
        clusters = new ArrayList<Cluster>(0);
    }

    // accessors

    public Cluster[] getClusters() {
        return clusters.toArray(new Cluster[clusters.size()]);
    }

    public void setClusters(Cluster[] clusters) {
        this.clusters = new ArrayList<Cluster>(clusters.length);
        for(Cluster cluster : clusters) {
            this.clusters.add(cluster);
        }
    }

    public void addCluster(Cluster cluster) {
        clusters.add(cluster);
    }

    public int count() {
        return clusters.size();
    }


    public int bestClustersCount() {
        int total = 0;
        for(Cluster cluster : clusters) {
            if(cluster.isBestCluster()) {
                total++;
            }
        }

        return total;
    }

    public void removeNotInBestClusters() {
        ArrayList<Cluster> toRemove = new ArrayList<Cluster>(clusters.size());
        for(Cluster cluster : clusters) {
            if(!cluster.isBestCluster()) {
                toRemove.add(cluster);
            }
        }

        for(Cluster cluster : toRemove) {
            clusters.remove(cluster);
        }
    }

    public boolean vectorInMedoids(Vector vector) {
        boolean  found = false;
        for(Cluster cluster : clusters) {
            if(cluster.getMedoid().sameAs(vector)) {
                return true;
            }
        }

        return false;
    }

    public void computeDeltas() {
        MedoidSet set = new MedoidSet();
        for(Cluster cluster : clusters) {
            set.addMedoid(cluster.getMedoid());
        }

        set.computeDeltas();
    }

    // comparable, writable

    public int compareTo(ClusterSet clusterSet) {
        if(clusterSet.getClusters().length == clusters.size()) {
            return 0;
        } else if(clusterSet.getClusters().length<clusters.size()) {
            return 1;
        } else {
            return -1;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(clusters.size());
        for(Cluster cluster : clusters) {
            cluster.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int numClusters = in.readInt();
        clusters = new ArrayList<Cluster>(numClusters);
        for(int i=0; i<numClusters; i++) {
            Cluster cluster = new Cluster();
            cluster.readFields(in);
            clusters.add(cluster);
        }
    }

    public Iterator<org.apache.mahout.clustering.Cluster> iterator() {
        return new ProClusClustersIterator(this);
    }
}
