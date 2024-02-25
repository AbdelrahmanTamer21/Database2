import java.io.Serializable;

public class Table implements Serializable {
    private String name;

    private Field[] fields;
    private String[] fileNames;

    public Table(String name, Field[] fields) {
        this.name = name;
        this.fields = fields;
    }
}
