import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Tuple implements Serializable {
    private Vector data = new Vector();
    private Hashtable <String, Object> htblColNameValue;
    public Tuple(Hashtable <String, Object> htblColNameValue){
        this.htblColNameValue = (Hashtable<String, Object>) htblColNameValue.clone();
    }

//    public Vector getData() {
//        return data;
//    }


    public Hashtable<String, Object> getHtblColNameValue() {
        return htblColNameValue;
    }

    @Override
    public String toString() {
        String str = "";
        Vector<String> v = new Vector<>();
        for (Map.Entry<String, Object> entry : getHtblColNameValue().entrySet())
            v.add(entry.getValue()+"");
        for (int i = 0; i < v.size(); i++) {
            if(i == v.size()-1)
                str+=v.get(i);
            else
                str+=v.get(i)+",";
        }
        return str;
    }
    public static void main (String[]args){
        Hashtable<String, Object> hash = new Hashtable<>();
        hash.put("id", new Integer(90));
        hash.put("Name", new String("Abdulbaki"));
        hash.put("number", "dddd");
        Tuple t = new Tuple(hash);
        System.out.println(t);
    }
}