package Main;

import java.io.Serializable;
import java.util.LinkedHashMap;

public class Tuple implements Serializable {
    private final LinkedHashMap<String,Object> values;

    //Name of the column
    private final String primaryKey;

    public Tuple(LinkedHashMap<String,Object> values, String primaryKey){
        this.values = values;
        this.primaryKey = primaryKey;
    }

    public LinkedHashMap<String,Object> getValues(){
        return values;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        for (String key : values.keySet()) {
            result.append(values.get(key)).append(",");
        }
        result.deleteCharAt(result.length()-1);
        return result.toString();
    }

    public Object getPrimaryKeyValue(){
        return values.getOrDefault(this.primaryKey,null);
    }
}
