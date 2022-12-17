package dbms.geraltigas.utils.cachedata;

public class CacheString implements CacheData {
    private String value;

    public CacheString(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
