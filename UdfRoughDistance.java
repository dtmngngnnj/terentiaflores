import java.lang.Math;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UdfRoughDistance extends UDF {

    /** Calculate the approximate distance between two points */ 
    public double evaluate(double lat1, double lon1, double lat2, double lon2) {

        // convert to radians
        lat1 = lat1 * Math.PI / 180.0;
        lon1 = lon1 * Math.PI / 180.0;
        lat2 = lat2 * Math.PI / 180.0;
        lon2 = lon2 * Math.PI / 180.0;

        double r = 6371.0; // radius of the earth in kilometer 
        double x = (lon2 - lon1) * Math.cos((lat1+lat2)/2.0);
        double y = (lat2 - lat1);
        return r*Math.sqrt(x*x+y*y);
    }

    /* The above formulas are called the "equirectangular approximation", 
     * to be used for small distances, if performance is more important 
     * than accuracy. 
     * See: http://www.movable-type.co.uk/scripts/latlong.html
     */
}

