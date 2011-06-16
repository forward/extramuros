package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 10:40
 */
public class ProClusDimensionSelectionReducer extends Reducer<Medoid,MedoidDimensionalData,Text,Medoid> {

    private static final Logger log = LoggerFactory.getLogger(ProClusDimensionSelectionReducer.class);


     @Override
    protected void reduce(Medoid medoid, Iterable<MedoidDimensionalData> values, Context context)
      throws IOException, InterruptedException {

         double counter = 0;
         MedoidDimensionalData averages = null;

         for(MedoidDimensionalData data : values) {
             counter++;
             if(averages == null) {
                 averages = data;
             } else {
                 averages.add(data.getData());
             }
         }

         averages.average(counter);


         try {
             log.info("COMPUTING DIMENSIONF FOR MEDOID:"+medoid.getLabel()+"\r\nAGGREGATED SUMS ("+averages.getData().length+"):"+averages.getData());
             medoid.computeDimension(averages.getData());

             context.write(new Text(medoid.getLabel()),medoid);
         } catch (Exception e) {
             log.error("Impossible to compute the dimensions for the medoid.",e);
         }

    }
}
