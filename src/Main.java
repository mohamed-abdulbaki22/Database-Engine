import java.io.*;
import java.util.*;

public class Main {
    public static int hash(double value) {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
        // Create a sample Hashtable
//        Hashtable<String, String> dataHashtable = new Hashtable<>();
//        dataHashtable.put("abc", "aabbcc");
//        dataHashtable.put("hh", "ncnc");
//
//        // Specify the CSV file name
//        String csvFileName = "metadata.csv";
//
//        try (PrintWriter pw = new PrintWriter(new File(csvFileName))) {
//            // Write the header (optional)
//            pw.println("Key,Value");
//
//            // Write each data entry
//            for (Map.Entry<String, String> entry : dataHashtable.entrySet()) {
//                pw.println(entry.getKey() + "," + entry.getValue());
//            }
//
//            System.out.println("Data written to " + csvFileName);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        Page c = DBApp.pageDeserialization();
//        System.out.println(c);
//        System.out.println(c.getTuples());
//        FileInputStream fileIn = new FileInputStream("Tables\\" + "wow" + ".ser");
//        ObjectInputStream In = new ObjectInputStream(fileIn);
//        Table t = (Table) In.readObject();
//        In.close();
//        fileIn.close();
//        System.out.println(t.getPages().get(0)+ t.getNumOfPages());
//
        String s = "3";
//        String t = "emad";
        int g = (int) 22222222222222L;

        System.out.println(hash(0.58));

//        bplustree index = new bplustree(2);
//        index.insert(2,3);
//        Table myTable = DBApp.DeserializeTable("wow");
//        for (String pageName: myTable.getPages()) {
//            Page p = myTable.pageDeserialization(pageName);
//            for (int i = 0; i < p.getTuples().size(); i++) {
//                Tuple t = p.getTuples().get(i);
////                int number = getNumber(strTableName, pageName);
////                double j = insertToIndex(getNumber(strTableName, pageName),i );
////                index.insert((Integer) t.getHtblColNameValue().get(strColName),j);
////                index.insert();
//                int value = (int) t.getHtblColNameValue().get("id");
//                index.insert(value, p.getPageNumber());
//            }
//        }
//        String strTableName = "meow";
//        DBApp	dbApp = new DBApp( );
//
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        dbApp.createTable( strTableName, "id", htblColNameType );
//
//
////        DBApp d = new DBApp();
////        d.createTable();
//        Table test = DBApp.DeserializeTable("meow");
//        System.out.println(test.getPages().toString());
//        Page p = test.pageDeserialization("meow1");
//        System.out.println(p.getTuples().toString());
//        System.out.println("cat".compareTo("caw") + new Integer(3).compareTo(4));
//        Hashtable <String,Object> ht = new Hashtable<>();
//        ht.put("Id",55);
//        ht.put("Name","Abdulbaki");
//        ht.put("Gender","Male");
//        System.out.println(ht.toString());
//        ht.put("Gender","Non-Binary");
//        System.out.println(ht.toString());
//        Tuple newTuple = new Tuple(ht);
//        System.out.println(ht.keySet());
////        bplustree b = DBApp.indexDeserialization("idIndex");
////        System.out.println(b.m);
        Page loadedPage = null;
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("pages\\"+"Student1"+".ser"))) {
            loadedPage = (Page) inputStream.readObject();
//            System.out.println("Loaded page content: " + loadedPage);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("Page 1 content = "+loadedPage.getTuples().size());
//        DBApp d = new DBApp();
//            String h = "5";
//            String ga = "5";
//        System.out.println(h == ga);
//        Table myTable = d.DeserializeTable("Student");
//        String type = myTable.getHtblColNameType().get("gpa");
//        System.out.println(type);
//        bplustreeDouble index = d.indexDeserializationDouble("gpaIndex");
//        System.out.println(index.search(0.95));
        DBApp sa = new DBApp();
//        DBApp.updateRecord("metadata.csv",6,new String []{"Student", "id", "java.lang.Integer", "True", "IdIndex", "B+Tree"});
        Object value = 3;
        Integer i = new Integer(3);
        System.out.println(value.equals(i));
        BTree idIndex = DBApp.indexDeserialization("nameIndex");
//        System.out.println("index is " +idIndex.search(0.88));
        System.out.println("columnIndex = "+ DBApp.chackForIndex("Student","id"));
        Table t = DBApp.DeserializeTable("Student");
        System.out.println(t.getPages());
//        File fileToDelete = new File("pages\\"+"meow1.ser");
//        fileToDelete.delete();
//        assertTrue(success);

//        BTree tree = new BTree<>();
//        ArrayList <Integer> l = new ArrayList<>();
//        l.add(1);
//        l.add(1);
//        tree.insert(1,l);
//        System.out.println(l+" " + l.get(1));
//        System.out.println(((ArrayList)tree.search(1)).size());
//        ArrayList f = ((ArrayList)tree.search(1));
//        f.add(4);
//        f.add(5);
//        System.out.println(((ArrayList)tree.search(1)));
//        System.out.println(DBApp.checkForColumnType("ohhh", "gpa"));
//        LinkedList list = new LinkedList<>();
//        list.add(2);
//        list.add(3);
//        Object arr[] = list.toArray();
//        System.out.println(Arrays.stream(arr).toArray());

//        BTree tree1 = DBApp.indexDeserialization("nameIndex");
//        System.out.println("awel test = "+tree1.search("Negm4"));
        BTree index = DBApp.indexDeserialization("IdIndex");
//            System.out.println("my key __________________" + htblColNameValue.get(currTable.getStrClusteringKeyColum()) );
//        System.out.println("my value __________________" + value );
        ArrayList <Integer> pages =(ArrayList<Integer>) index.search((Comparable) value);
        System.out.println(pages);
    }
}
