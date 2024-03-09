import java.io.Serializable;

public class Tuple implements Serializable {
    private final String[] values;

    public Tuple(String[] values){
        this.values = values;
    }

    public String[] getValues(){
        return values;
    }
    public String toString() {
        StringBuilder result = new StringBuilder();
        for(String val:values){
            result.append(val).append(",");
        }
        if(!result.isEmpty()) {
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }
}
