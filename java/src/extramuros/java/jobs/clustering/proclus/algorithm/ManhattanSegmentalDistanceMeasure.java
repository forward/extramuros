package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 09:42
 */
public class ManhattanSegmentalDistanceMeasure extends ManhattanDistanceMeasure {

    public static double distance(Medoid m, Vector v) {
        double[] medoidComponents = m.elementsInDimensions();
        double[] vectorComponents = m.elementsInDimensions(v);

        return distance(medoidComponents, vectorComponents);
    }

    public static double distance(Vector a, Vector b, Medoid dimensions){
        double[] aComponents = dimensions.elementsInDimensions(a);
        double[] bComponents = dimensions.elementsInDimensions(b);

        return distance(aComponents, bComponents);
    }

}
