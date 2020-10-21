package smile.demo.atypon_and;

import java.io.Serializable;
import java.util.List;

import smile.data.DataFrame;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.neighbor.*;

/**
 * Linear search based on AuthorPubDistance
 *
 * @author Omiros Metaxas
 */


public class AuthorPubNNSearch<Tuple> implements NearestNeighborSearch<Tuple, Tuple>, RNNSearch<Tuple, Tuple>, Serializable {

    private static final long serialVersionUID = 2L;

    /**
     * The dataset of search space.
     */
    private DataFrame data;
    /**
     * The distance function used to determine nearest neighbors.
     */
    private AuthorPubDistance distance;

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

    /**
     * Constructor. By default, query object self will be excluded from search.
     */
    public AuthorPubNNSearch(DataFrame dataset) {
        this.data = dataset;
        this.distance = new AuthorPubDistance(dataset);
        if (!schema.equals(dataset.schema())) {
            throw new IllegalArgumentException( String.format("Schema should have the following format (%s)", schema.toString()));
        }
    }

    @Override
    public String toString() {
        return String.format("Linear Search (%s)", distance);
    }

    @Override
    public Neighbor<Tuple, Tuple> nearest(Tuple q) {
        // avoid Stream.reduce as we will create a lot of temporary Neighbor objects.
        //double[] dist =     Arrays.stream(data).parallel().mapToDouble(x -> distance.d(q, x)).toArray();
        double[] dist =    data.stream().parallel().mapToDouble(x -> distance.d((smile.data.Tuple) q, x)).toArray();

        int index = -1;
        double nearest = Double.MAX_VALUE;
        for (int i = 0; i < dist.length; i++) {
            if (dist[i] < nearest && q != data.get(i)) {
                index = i;
                nearest = dist[i];
            }
        }

        return (Neighbor<Tuple, Tuple>) Neighbor.of(data.get(index), index, nearest);
    }
    /*
        @Override
        @SuppressWarnings("unchecked")
        public Neighbor<T, T>[] knn(T q, int k) {
            if (k <= 0) {
                throw new IllegalArgumentException("Invalid k: " + k);
            }

            if (k > data.length) {
                throw new IllegalArgumentException("Neighbor array length is larger than the data size");
            }

            double[] dist = Arrays.stream(data).parallel().mapToDouble(x -> distance.d(q, x)).toArray();
            HeapSelect<NeighborBuilder<T, T>> heap = new HeapSelect<>(k);
            for (int i = 0; i < k; i++) {
                heap.add(new NeighborBuilder<>());
            }

            for (int i = 0; i < dist.length; i++) {
                NeighborBuilder<T, T> datum = heap.peek();
                if (dist[i] < datum.distance && q != data[i]) {
                    datum.distance = dist[i];
                    datum.index = i;
                    datum.key = data[i];
                    datum.value = data[i];
                    heap.heapify();
                }
            }

            heap.sort();
            return Arrays.stream(heap.toArray()).map(NeighborBuilder::toNeighbor).toArray(Neighbor[]::new);
        }
    */
    @Override
    public void range(Tuple q, double radius, List<Neighbor<Tuple, Tuple>> neighbors) {
        if (radius <= 0.0) {
            throw new IllegalArgumentException("Invalid radius: " + radius);
        }
        //double[] dist = Arrays.stream(data).parallel().mapToDouble(x -> distance.d(q, x)).toArray();
        double[] dist =    data.stream().parallel().mapToDouble(x -> distance.d((smile.data.Tuple) q, x)).toArray();
        for (int i = 0; i < data.nrows(); i++) {
            if (dist[i] <= radius && q != data.get(i)) {
                neighbors.add((Neighbor<Tuple, Tuple>) Neighbor.of(data.get(i), i, dist[i]));
            }
        }
    }
}

