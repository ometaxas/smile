package smile.demo.atypon_and;

import org.apache.commons.csv.CSVFormat;
import smile.clustering.DBSCAN;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.io.Read;

import smile.neighbor.RNNSearch;

import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.util.Paths;



public class TestDBScan {



    public static void main(String argv[]) {
        //var df = Read.parquet("/media/datadisk/Datasets/MAG_S2/S2_MAG_fos_ACM_outer_df.parquet");

        try {



            //   JSON json = new JSON().mode(JSON.Mode.MULTI_LINE);
            //   DataFrame df = json.read("/home/ometaxas/Projects/Smile/TestJava/test.json");
            // var df = Read.json("/home/ometaxas/Projects/Smile/TestJava/test.json");


            var format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
            StructType schema = DataTypes.struct(
                    new StructField("paperid", DataTypes.StringType),
                    new StructField("doi", DataTypes.StringType),
                    new StructField("shortnormname", DataTypes.StringType),
                    new StructField("soundex", DataTypes.StringType),
                    new StructField("normname", DataTypes.StringType),
                    new StructField("magid", DataTypes.StringType),
                    new StructField("s2id", DataTypes.StringType),
                    new StructField("lvl0", DataTypes.StringType),
                    new StructField("lvl1", DataTypes.StringType),
                    new StructField("affid", DataTypes.StringType)

            );


            DataFrame acmtest = Read.csv("./demo/src/main/java/smile/demo/atypon_and/acmsmall.csv", format, schema);



            if (!schema.equals(acmtest.schema())) {
                System.out.println(acmtest.schema().toString());
                System.out.println(schema.toString());
            }


            RNNSearch<Tuple,Tuple> search = new AuthorPubNNSearch(acmtest);

            DBSCAN dbscan = DBSCAN.fit(acmtest, search, 2, 0.4);
            System.out.println(dbscan.toString());

        }
        catch(Exception e) {
            System.out.println(e.toString());

        }
        //ScatterPlot.plot(x, clusters.y, '.', Palette.COLORS).window();

    }
}
