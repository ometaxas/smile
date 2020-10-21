package smile.demo.atypon_and;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.math.distance.Distance;

/**
 * The distance between author - pub pairs.
 *
 * @author Omiros Metaxas
 */

public class AuthorPubDistance implements Distance<Tuple> {

    private DataFrame df;


    public AuthorPubDistance(DataFrame df) {
        this.df = df;
    }

    @Override
    public String toString() {
        return String.format("Author pub distance on %s", df);
    }

    /**
     * Compute the distance between two tuples
     */
    public double d(int x, int y) {
        return d(df.get(x), df.get(y));
    }

    /**
     * Compute the distance between two tuples
     *   StructType schema = DataTypes.struct(
     *                     new StructField("paperid", DataTypes.StringType),
     *                     new StructField("doi", DataTypes.StringType),
     *                     new StructField("shortnormname", DataTypes.StringType),
     *                     new StructField("soundex", DataTypes.StringType),
     *                     new StructField("normname", DataTypes.StringType),
     *                     new StructField("magid", DataTypes.StringType),
     *                     new StructField("s2id", DataTypes.StringType),
     *                     new StructField("lvl0", DataTypes.StringType),
     *                     new StructField("lvl1", DataTypes.StringType),
     *                     new StructField("affid", DataTypes.StringType)
     *
     *             );
     */
    @Override
    public double d(Tuple x, Tuple y) {
        if (x.schema() != y.schema()) {
            throw new IllegalArgumentException("Tuples have different schemas ");
        }
        double score = 0;
        double sum = 0;
        if (x.get("magid")!=null && y.get("magid")!=null) {
            sum+=1;
            if (x.get("magid").equals(y.get("magid")))
                score += 1;
        }
        if (x.get("s2id")!=null && y.get("s2id")!=null) {
            sum += 1;
            if (x.get("s2id").equals(y.get("s2id")))
                score += 1;
        }
        if (x.get("shortnormname")!=null && y.get("shortnormname")!=null ) {
            sum += 0.5;
            if (x.get("shortnormname").equals(y.get("shortnormname")))
                score += 0.5;
        }
        if (x.get("normname")!=null && y.get("normname")!=null ) {
            sum += 0.5;
            if (x.get("normname").equals(y.get("normname")))
                score += 0.5;
        }
        if (x.get("lvl0")!=null && y.get("lvl0")!=null ) {
            sum += 0.5;
            if (x.get("lvl0").equals(y.get("lvl0")) && !x.get("lvl0").toString().isBlank() && !y.get("lvl0").toString().isBlank())
                score += 0.5;
        }
        if (x.get("lvl1")!=null && y.get("lvl1") != null ) {
            sum += 0.5;
            if (x.get("lvl1").equals(y.get("lvl1")) && !x.get("lvl1").toString().isBlank() && !y.get("lvl1").toString().isBlank())
                score += 0.5;
        }
        if (x.get("affid")!=null && y.get("affid")!=null) {
            sum += 1;
            if (x.get("affid").equals(y.get("affid")) && !x.get("affid").toString().isBlank() && !y.get("affid").toString().isBlank())
                score += 1;
        }




        return 1 - score/sum;

    }
}


