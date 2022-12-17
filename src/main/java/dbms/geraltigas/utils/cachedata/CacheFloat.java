package dbms.geraltigas.utils.cachedata;

public class CacheFloat implements CacheData{
    float value;

    public CacheFloat(float value) {
        this.value = value;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
