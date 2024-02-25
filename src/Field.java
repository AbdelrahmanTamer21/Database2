public class Field{

    private String columnName;
    private String columnType;
    private boolean clusteringKey;
    private String indexName;
    private String indexType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public boolean isClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(boolean clusteringKey) {
        this.clusteringKey = clusteringKey;
    }
}
