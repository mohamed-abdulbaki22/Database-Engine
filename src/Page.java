import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private Vector <Tuple> tuples;
    private String name;

    private int pageNumber;

    public int getPageNumber() {
        return pageNumber;
    }
//    private String dataString;

    public String getName() {
        return name;
    }

    public Page(String name) {
        this.tuples = new Vector<>();
        this.name = name;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }
//    public String getDataString() {
//        return dataString;
//    }

//    private void setDataString(String dataString) {
//        this.dataString = dataString;
//    }


    public void insertTuple(Tuple tuple){
        getTuples().add(tuple);
//        setDataString( getDataString() + tuple.getData() + ", ");
    }

    @Override
    public String toString()
    {
//        return getDataString();
        String res="";
        for (int i = 0; i < tuples.size(); i++) {
//            if (i == tuples.size()-1) res+=tuples.get(i).toString();
//            else
            res+=tuples.get(i).toString() + ", ";
        }
        return res;
    }


    public static void main(String[] args) {
        // Create a sample page with tuples
        Page page = new Page("ss");
        Hashtable <String, Object> hash = new Hashtable<>();
        hash.put("id", new Integer(90));
        hash.put("name", new String("Abdulbaki"));
        hash.put("gpa", new Double( 0.22));
        page.insertTuple(new Tuple(hash));
        hash.clear();
        hash.put("id", new Integer( 2343432 ));
        hash.put("name", new String("Ahmed Noor" ) );
        hash.put("gpa", new Double( 0.95 ) );
        page.insertTuple(new Tuple(hash));
        // Serialize the page and save it to a file
//        page.insertTuple(new Tuple(1,2,"Ali","wow"));
//        page.insertTuple(new Tuple(3,4,"55-23897","wow"));
//        page.insertTuple(new Tuple(1,2,"boda","meow"));
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("page.ser"))) {
            outputStream.writeObject(page);
            System.out.println("Page serialized and saved successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialize the page from the file
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("page.ser"))) {
            Page loadedPage = (Page) inputStream.readObject();
            System.out.println("Loaded page content: " + loadedPage);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(page);
        System.out.println(page.getTuples().toString());
    }

    public void setTuples(Vector<Tuple> newTuples) {
        tuples = newTuples;
    }

    public void setName(String newPageName) {
        this.name = newPageName;
    }
}
