package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 09:22
 */
public class Medoid implements Writable, WritableComparable<Medoid> {

    private static final Logger log = LoggerFactory.getLogger(Medoid.class);

    // The point where this medoid is centered
    private Vector vector;

    // The dimensions of the subspace for this medoid
    private int[] dimensions;

    // weights associated to each dimension
    protected double[] weightedDimensions;

    // Min. distance from this medoid to any other medoid in
    // the medoid set.
    private double delta;

    // Label identifying this medoid
    private String label;


    public Medoid() {
        setVector(null);
        setLabel("");
        setDelta(0);
        setDimensions(new int[0]);
        weightedDimensions = null;
    }

    public Medoid(Vector v) {
        setVector(v);
        setLabel("");
        setDelta(0);
        int numDimensions = v.getNumNondefaultElements();
        setDimensions(new int[numDimensions]);
        weightedDimensions = null;

        for(int i=0; i<numDimensions; i++) {
            getDimensions()[i] = i;
        }
    }

    public Medoid(Vector v, int[] dims) {
        setVector(v);
        this.setDimensions(dims);
    }

    public void addDimension(int dimension) {
        setDimensions(Arrays.copyOf(getDimensions(), getDimensions().length+1));
        getDimensions()[getDimensions().length-1] = dimension;
        Arrays.sort(getDimensions());
    }

    public void clearWeightedDimensions() {
        weightedDimensions = null;
    }

    public void clearDimensions() {
        setDimensions(new int[0]);
    }

    public void write(DataOutput out) throws IOException {
        Text writableLabel = new Text(label);

        writableLabel.write(out);
        new VectorWritable(getVector()).write(out);

        out.writeDouble(delta);

        out.writeInt(getDimensions().length);

        for(int dimension : getDimensions()) {
            out.writeInt(dimension);
        }

        if(weightedDimensions!=null) {
            out.writeBoolean(true);
            out.writeInt(weightedDimensions.length);
            for(double weightedDimension : weightedDimensions) {
                out.writeDouble(weightedDimension);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public void readFields(DataInput in) throws IOException {
        Text writableLabel = new Text();
        writableLabel.readFields(in);
        setLabel(writableLabel.toString());

        VectorWritable writable = new VectorWritable();
        writable.readFields(in);
        setVector(writable.get());

        setDelta(in.readDouble());

        int numDimensions = in.readInt();

        setDimensions(new int[numDimensions]);

        for(int i=0; i<numDimensions; i++) {
            getDimensions()[i] = in.readInt();
        }

        boolean hasWeightedDimensions = in.readBoolean();
        if(hasWeightedDimensions) {
            numDimensions = in.readInt();
            weightedDimensions = new double[numDimensions];
            for(int i=0; i<numDimensions; i++) {
                weightedDimensions[i] = in.readDouble();
            }
        }  else {
            weightedDimensions = null;
        }
    }

    public double[] elementsInDimensions() {
        double[] elements = new double[getDimensions().length];

        for(int i=0; i< getDimensions().length; i++) {
            elements[i] = getVector().get(getDimensions()[i]);
        }

        return elements;
    }

    public double[] elementsInDimensions(Vector v) {
        double[] elements = new double[getDimensions().length];

        for(int i=0; i< getDimensions().length; i++) {
            elements[i] = v.get(getDimensions()[i]);
        }

        return elements;
    }

    public double segmentalDistanceTo(Vector v) {
        return ManhattanSegmentalDistanceMeasure.distance(this,v);
    }

    public double distanceTo(Vector v) {
        EuclideanDistanceMeasure dist = new EuclideanDistanceMeasure();
        return dist.distance(getVector(),v);
    }

    public double[] dimensionDistances(Vector v) throws Exception {
        if(v.getNumNondefaultElements()!=getVector().getNumNondefaultElements()) {
            throw(new Exception("Cannot compute the distance for the components of vectors in different dimensions"));
        }

        double[] dists = new double[v.getNumNondefaultElements()];
        for(int i=0; i<dists.length; i++) {
            double dim_i = getVector().get(i);
            double dim_j = v.get(i);

            dists[i] = Math.sqrt(Math.pow((dim_i - dim_j),2));
        }

        return dists;
    }

    public double[] segmentalDimensionDistances(Vector v) throws Exception {
        if(v.getNumNondefaultElements()!=getVector().getNumNondefaultElements()) {
            throw(new Exception("Cannot compute the distance for the components of vectors in different dimensions"));
        }

        double[] dists = new double[v.getNumNondefaultElements()];
        for(int i=0; i<dists.length; i++) {
            double dim_i = getVector().get(i);
            double dim_j = v.get(i);

            dists[i] = Math.abs(dim_i - dim_j);
        }

        return dists;
    }
    /*
       locality_i (L_i) -> points within distance Delta of this medoid
     */
    public boolean inLocality(Vector point) {
        double distanceToPoint = distanceTo(point);
        return distanceToPoint <= getDelta();
    }


    public void computeDimension(double[] aggregatedDistancesForLiPoints) throws Exception {
        if(aggregatedDistancesForLiPoints.length != getVector().getNumNondefaultElements()) {
            throw(new Exception("The number of aggregated dimensions does not match the dimensions of the medoid:"+
                    aggregatedDistancesForLiPoints.length +" vs "+getVector().getNumNondefaultElements()));
        }

        double y = computeY(aggregatedDistancesForLiPoints);
        log.info(" - computed Y: "+y);

        double ro = computeRo(aggregatedDistancesForLiPoints, y);
        log.info(" - computed Ro: "+ro);

        weightedDimensions = new double[aggregatedDistancesForLiPoints.length];
        for(int i=0; i<weightedDimensions.length; i++) {
            double distance = aggregatedDistancesForLiPoints[i];
            weightedDimensions[i] = (distance - y) / ro;
        }
    }

    /*
      Ro_i = Sqrt( Sum( (Xij - Yi)^2 ) / (NumDims - 1))
     */
    private double computeRo(double[] distances, double y) {
        double sum = 0;

        for(double distance : distances) {
            sum = sum + Math.pow((distance - y),2);
        }

        return Math.sqrt((sum/(distances.length-1)));

    }

    /*
      Yi = Sum(j=0,numDims)[Xij]/numDims | Xij = avg_dist(PointsL_i to Medoid_i for Dim j)
     */
    protected double computeY(double[] distances) {
        double sum = 0;
        for(double distance : distances) {
            sum = sum + distance;
        }
        return sum/distances.length;
    }

    public MedoidDimension[] reifyDimensions() {
        int numDimensions = getVector().getNumNondefaultElements();
        MedoidDimension[] out = new MedoidDimension[numDimensions];

        for(int i=0; i<numDimensions; i++){
            out[i] = new MedoidDimension(this, i, weightedDimensions[i]);
        }

        return out;
    }

    /*
      Accessors
     */

    public Vector getVector() {
        return vector;
    }

    public void setVector(Vector vector) {
        this.vector = vector;
    }

    public double getDelta() {
        return delta;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    // comparable

    public boolean sameAs(Vector vector) {
        for(int i=0; i<getVector().getNumNondefaultElements(); i++) {
            if(getVector().get(i) != vector.get(i)) {
                return false;
            }
        }
        return true;
    }

    public int compareTo(Medoid medoid) {
        if(medoid.getVector().getNumNondefaultElements() < getVector().getNumNondefaultElements()) {
            return 1;
        } else if(medoid.getVector().getNumNondefaultElements() > getVector().getNumNondefaultElements()) {
            return -1;
        } else {
            for(int i=0; i<medoid.getVector().getNumNondefaultElements(); i++){
                if(medoid.getVector().get(i)< getVector().get(i)) {
                    return 1;
                } else if(medoid.getVector().get(i) > getVector().get(i)) {
                    return -1;
                }
            }

            return 0;
        }
    }

    public int[] getDimensions() {
        return dimensions;
    }

    public void setDimensions(int[] dimensions) {
        this.dimensions = dimensions;
    }
}
