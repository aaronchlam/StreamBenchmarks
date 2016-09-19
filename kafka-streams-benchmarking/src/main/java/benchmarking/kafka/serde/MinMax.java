package benchmarking.kafka.serde;

/**
 * Created by jeka01 on 08/09/16.
 */
public class MinMax {
    Long ts;


    Double maxPrice;
    Double minPrice;

    public MinMax(Long ts, Double maxPrice, Double minPrice) {
        this.ts = ts;
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(Double maxPrice) {
        this.maxPrice = maxPrice;
    }

    public Double getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(Double minPrice) {
        this.minPrice = minPrice;
    }

}

