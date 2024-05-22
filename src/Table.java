import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String strTableName;
    private String strClusteringKeyColum;
    private Hashtable<String,String> htblColNameType;

    private Vector<String> pages = new Vector<String>() ;
    private int numOfPages ;

    public Table(String strTableNam,String strClusteringKeyColum,
                 Hashtable<String,String>htblColNameType) {                        //made by me
        this.strTableName = strTableNam;
        this.strClusteringKeyColum= strClusteringKeyColum;
        this.htblColNameType = htblColNameType;
        numOfPages = 1;

    }

    public String getStrTableName() {
        return strTableName;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public String getStrClusteringKeyColum() {
        return strClusteringKeyColum;
    }

    public void setStrClusteringKeyColum(String strClusteringKeyColum) {
        this.strClusteringKeyColum = strClusteringKeyColum;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }
    public Vector<String> getPages() {
        return pages;
    }

    public int getNumOfPages() {
        return numOfPages;
    }

    public void setNumOfPages(int numOfPages) {
        this.numOfPages = numOfPages;
    }

    public static void pageSerialization(Page page){
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("pages\\"+page.getName()+".ser"))) {
            outputStream.writeObject(page);
            System.out.println("Page serialized and saved successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Page pageDeserialization(String pageName) {
        Page loadedPage = null;
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("pages\\"+pageName+".ser"))) {
            loadedPage = (Page) inputStream.readObject();
//            System.out.println("Loaded page content: " + loadedPage);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return loadedPage;
    }
}
