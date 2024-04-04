package Main;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedHashMap;

public class Tuple implements Serializable {
    private Hashtable<String,Object> values;
    private String primaryKey;

    public Tuple(Hashtable<String,Object> values, String primaryKey){
        this.values = values;
        this.primaryKey = primaryKey;
    }

    public Hashtable<String,Object> getValues(){
        return values;
    }
    public void setValues(Hashtable<String,Object> values){
        this.values = values;
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
        return values.get(primaryKey);
    }
}
