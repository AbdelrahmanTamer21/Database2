import java.io.Serializable;

public class Table implements Serializable {
    private String tableName;

    private String[] fileNames;

    public Table(String name) {
        this.tableName = name;
    }

    public void insertTuple(){

    }
}
