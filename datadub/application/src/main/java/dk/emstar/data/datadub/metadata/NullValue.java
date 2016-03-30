package dk.emstar.data.datadub.metadata;

public class NullValue {

    public static NullValue instance = new NullValue();

    @Override
    public String toString() {
        return "<null>";
    }

}
