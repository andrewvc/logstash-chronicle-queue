package co.elastic.logstash;

/**
 * Created by andrewvc on 7/26/15.
 */
public interface Converter<T> {
    public byte[] serialize(T item);
    public T deserialize(byte[] bytes);
}
