
/** * @author Wael Abouelsaadat */

import java.io.*;
import java.util.*;


public class DBApp {



    public DBApp( ){

    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init( ){


    }

    public static void writeToMetaData(String strTableName,
                                       String strClusteringKeyColumn,
                                       Hashtable<String, String> htblColNameType) throws FileNotFoundException {
        try (FileWriter fw = new FileWriter(new File("metadata.csv"),true)) {
//            fw.write("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType "); // Write header

            for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
                fw.write(""+"\n"+strTableName +","+ entry.getKey() + "," + entry.getValue() + "," + (((entry.getKey()).equals(strClusteringKeyColumn))?"True":"False")+ "," +null+ "," +null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table DeserializeTable(String strTableName) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("Tables\\" + strTableName + ".ser");
        ObjectInputStream In = new ObjectInputStream(fileIn);
        Table t = (Table) In.readObject();
        In.close();
        fileIn.close();
        return t;
    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
        // Create a new table
        Table newTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

        // Write metadata to CSV
        writeToMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
        Page p1 = new Page(strTableName + newTable.getNumOfPages());
        newTable.getPages().add(p1.getName());
        Table.pageSerialization(p1);
        // Serialize and save the table
        FileOutputStream fileOut = new FileOutputStream("Tables\\" + strTableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(newTable);
        out.close();
        fileOut.close();

        System.out.println("Serialized data is saved in " + strTableName + ".ser");
    }


    public static void indexSerialization(BTree index, String strIndexName){
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("Index\\"+strIndexName+".ser"))) {
            outputStream.writeObject(index);
            System.out.println("Index serialized and saved successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static BTree indexDeserialization(String strIndexName){
        BTree loadedIndex = null;
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("Index\\"+strIndexName+".ser"))) {
            loadedIndex = (BTree) inputStream.readObject();
//            System.out.println("Loaded page content: " + loadedPage);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return loadedIndex;
    }


    // following method creates a B+tree index
    public void createIndex(String   strTableName,
                            String   strColName,
                            String   strIndexName) throws DBAppException, IOException, ClassNotFoundException {

        Table myTable = DeserializeTable(strTableName);
//        String type = myTable.getHtblColNameType().get(strColName);
            BTree index = new BTree<>();
            for (int pn = 0; pn < myTable.getPages().size(); pn++) {
                String pageName = myTable.getPages().get(pn);
                Page p = myTable.pageDeserialization(pageName);
                int count=0;
                for (int i = 0; i < p.getTuples().size(); i++) {
                    Tuple t = p.getTuples().get(i);
                    count++;
//                int number = getNumber(strTableName, pageName);
                    Comparable key = (Comparable) t.getHtblColNameValue().get(strColName);
                    ArrayList <Integer>listOfPages;
                    if (index.search(key) == null) {
                        listOfPages = new ArrayList<>();
                        listOfPages.add(pn+1);
                        index.insert(key,listOfPages);
                    }else{
                        listOfPages = (ArrayList<Integer>) index.search(key);
                        if (listOfPages.get(listOfPages.size()-1)!= pn+1) {
                            listOfPages.add(pn+1);
                        }
                    }
                }
            }
            indexSerialization(index, strIndexName);
            int row = -1;
            String dataType = null;
            String isClusteringKey = "False";
            String line = "";
            String splitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
            while ((line = br.readLine()) != null){
                row++;
                String[] tableData = line.split(splitBy);
                if(tableData[0].equals(strTableName)&& strColName.equals(tableData[1])) {
                    dataType = tableData[2];
                    isClusteringKey = tableData[3];
                    break;
                }
            }
            DBApp.updateRecord("metadata.csv",row,new String []{strTableName, strColName, dataType,isClusteringKey, strIndexName, "B+tree"});
    }

        public static void updateRecord(String filePath, int recordIndex, String[] newData) {
            // Read the CSV file and store its content in a list
            List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return; // Exit if there's an error reading the file
            }

            // Check if the record index is valid
            if (recordIndex >= 0 && recordIndex < lines.size()) {
                // Update the record with new data
                lines.set(recordIndex, String.join(",", newData)); // Assuming CSV uses comma as delimiter
            } else {
                System.err.println("Invalid record index to update.");
                return; // Exit if the record index is invalid
            }

            // Write the modified content back to the CSV file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
                for (String line : lines) {
                    writer.write(line);
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
                return; // Exit if there's an error writing to the file
            }

            System.out.println("Record at index " + recordIndex + " updated successfully.");
        }



    //    int getNumber(String table, String page){
//        int n = 0;
//        String s = "";
//        for (int i = table.length()-1; i < page.length() ; i++) {
//            s+=page.charAt(i);
//        }
//        n = Integer.parseInt(s);
//        return n;
//    }
    public double insertToIndex(int pageNum,int index){
        String str="";
        str= pageNum + "." + index + "1";
        return Double.parseDouble(str);
    }
//    public double insertToIndex(int pageNum,String index){
//        String str="";
//
//        str= pageNum + "." + index + "1";
//        return Double.parseDouble(str);
//    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key

    public static boolean checkTableExists(String strTableName) throws IOException {
        boolean flag = false;
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null) {
            String[] tableData = line.split(splitBy);
            if (tableData[0].equals(strTableName))
                flag = true;
        }
        return flag;
    }
    public static boolean checkColumnExists(String strTableName,String colName) throws IOException {
        boolean flag = false;
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null) {
            String[] tableData = line.split(splitBy);
            if (tableData[0].equals(strTableName) && tableData[1].equals(colName))
                flag = true;
        }
        return flag;
    }

    public static boolean chackForIndex(String strTableName, String strColName) throws IOException {
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null){
            String[] tableData = line.split(splitBy);
            if(tableData[0].equals(strTableName)&&strColName.equals(tableData[1])){
                if(!tableData[4].equals("null")) return true;
                else break;
            }
        }
        return false;
    }

    public static String getIndexName(String strTableName, String strColName) throws IOException {
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null){
            String[] tableData = line.split(splitBy);
            if(tableData[0].equals(strTableName)&&strColName.equals(tableData[1])) return tableData[4];
        }
        return null;
    }
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {

        try {
            //  CHECK IF THE TABLE EXITS FIRST
            if (!checkTableExists(strTableName))
                throw new DBAppException("Table does not exist");

            Table table = DeserializeTable(strTableName);
            Set<String> columnNames = htblColNameValue.keySet(); // 34an tgeb el keys
//                    Vector<String> keys = new Vector<>();
            for (String columnName : columnNames) {
                if (!(checkColumnExists(strTableName, columnName)))
                    throw new DBAppException("Some columns do not exist in this table");
            }
            String line = "";
            String splitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
            while ((line = br.readLine()) != null) {
                String[] tableData = line.split(splitBy);
                if (tableData[0].equals(strTableName)) {
                    String type = tableData[2];
                    switch (type) {
                        case "java.lang.Integer" -> {
                            if (!(htblColNameValue.get(tableData[1]) instanceof Integer))
                                throw new DBAppException("Wrong data type");
                        }
                        case "java.lang.double" -> {
                            if (!(htblColNameValue.get(tableData[1]) instanceof Double))
                                throw new DBAppException("Wrong data type");
                        }
                        case "java.lang.String" -> {
                            if (!(htblColNameValue.get(tableData[1]) instanceof String))
                                throw new DBAppException("Wrong data type");
                        }
                    }
                }

            }
            if (htblColNameValue.get(table.getStrClusteringKeyColum()) == null)
                throw new DBAppException("Missing clustering key");
            Comparable var = (Comparable) htblColNameValue.get(table.getStrClusteringKeyColum());
            int i = 0, pageNumber = 0;
//            Hashtable<String,Object> temp = null;
//            Tuple tuple;
            boolean flag = false;
            for (pageNumber = 0; pageNumber < table.getPages().size(); pageNumber++) {
                String pageName = table.getPages().get(pageNumber);
                Page p = table.pageDeserialization(pageName);

                for (i = 0; i < p.getTuples().size(); i++) {
                    Tuple t = p.getTuples().get(i);
                    if(((Comparable) t.getHtblColNameValue().get(table.getStrClusteringKeyColum())).compareTo(var) == 0){
                        throw new DBAppException("There already exists a record with the same clustering key ");
                    }
                    else if (((Comparable) t.getHtblColNameValue().get(table.getStrClusteringKeyColum())).compareTo(var) > 0) {
                        flag = true;
                        break;
                    }

                }
//                System.out.println(pn);
                if (flag)
                    break;
            }

            if (!flag)
                pageNumber--;
//            i--;
            String pageName = table.getPages().get(pageNumber);
            Page pa = table.pageDeserialization(pageName);
            Vector<Tuple> newTuples = new Vector<Tuple>();
            Tuple temp = null;
            boolean isInserted = false;
            for (int c = 0; c < i; c++) {
                newTuples.add(pa.getTuples().get(c));
            }
            newTuples.add(new Tuple(htblColNameValue));
            for (int c = i; c < pa.getTuples().size(); c++) {
                if (c == 19) {
                    temp = pa.getTuples().get(c);
                    isInserted = true;
                    break;
                }
                newTuples.add(pa.getTuples().get(c));
            }
            pa.setTuples(newTuples);
            table.pageSerialization(pa);
            newTuples.clear();
            System.out.println("table size" + table.getPages().size());
            for (int x = pageNumber + 1; x < table.getPages().size() && isInserted; x++) {
                String name = table.getPages().get(x);
                Page p = table.pageDeserialization(name);
                newTuples.add(temp);
                isInserted = false;
//                System.out.println("size = "+ p.getTuples().size());
                for (int y = 0; y < p.getTuples().size(); y++) {
                    if (y == 19) {
                        temp = p.getTuples().get(y);
                        isInserted = true;
//                        System.out.println("y is "+ y);
                        break;
                    }
                    newTuples.add(p.getTuples().get(y));
                }
                p.setTuples(newTuples);
                table.pageSerialization(p);
                newTuples.clear();

            }
//            System.out.println("y value is = "+y);
            if (isInserted) {
                Page page = new Page(table.getStrTableName() + "" + (table.getPages().size() + 1));
                String newPageName = new String(strTableName + "" + (table.getPages().size() + 1));
                table.getPages().add(newPageName);
                System.out.println("el page el gededa " + table.getPages());
                System.out.println("el tuple " + temp);
                page.getTuples().add(temp);
                table.pageSerialization(page);
                // Serialize and save the table
                FileOutputStream fileOut = new FileOutputStream("Tables\\" + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(table);
                out.close();
                fileOut.close();


            }
            String line2 = "";
            String splitBy2 = ",";
            BufferedReader br2 = new BufferedReader(new FileReader("metadata.csv"));
            while ((line2 = br2.readLine()) != null){
                String[] tableData = line.split(splitBy);
                if(tableData[0].equals(strTableName)){
                    String colName = tableData[1];
                    if(chackForIndex(strTableName,colName)){
                        String indexName = getIndexName(strTableName,colName);
                        createIndex(strTableName,colName,indexName);
//                        BTree index = indexDeserialization(indexName);
//                        Comparable key = (Comparable) htblColNameValue.get(colName);
//                        ArrayList <Integer>listOfPages;
//                        if (index.search(key) == null) {
//                            listOfPages = new ArrayList<>();
//                            listOfPages.add(pageNumber);
//                            index.insert(key,listOfPages);
//                        }else{
//                            listOfPages = (ArrayList<Integer>) index.search(key);
//                            boolean alreadyExists = false;
////                            for(int j=0;j<listOfPages.size();j++){
////                                if(listOfPages.get(j).equals()){}
////                            }
//                                if (listOfPages.contains()) {
//                                }
//                        }
                    }
                }
            }

        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getClusteringKey(String strTableName) throws IOException {
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null) {
            String[] tableData = line.split(splitBy);
            if (tableData[0].equals(strTableName) && tableData[3].equals("True")) {
                return tableData[1];
            }
        }
        return null;
    }

    public static int binarySearch(String strTableName, Page page , Comparable value) throws IOException {
        int left = 0;
        int right = page.getTuples().size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            Tuple t = page.getTuples().get(mid);
            String clusteringKey = getClusteringKey(strTableName);
            // Check if target is present at mid
            System.out.println("Roberto Ferminio ____ "+clusteringKey+"  ______"+t.getHtblColNameValue().get(clusteringKey));
            if (((Comparable) t.getHtblColNameValue().get(clusteringKey)).compareTo(value) == 0) {
                return mid;
            }
            if (((Comparable) t.getHtblColNameValue().get(clusteringKey)).compareTo(value) > 0) {
                right = mid - 1;
            } else
                left = mid + 1;
        }
        return -1;
    }
//
//    public static int binarySearchDouble(String strTableName, Page page , Double value) throws IOException {
//        int left = 0;
//        int right = page.getTuples().size() - 1;
//        while (left <= right) {
//            int mid = left + (right - left) / 2;
//            Tuple t = page.getTuples().get(mid);
//            String clusteringKey = getClusteringKey(strTableName);
//            // Check if target is present at mid
//            if (((Double) t.getHtblColNameValue().get(clusteringKey)) == value) {
//                return mid;
//            }
//            if (((Double) t.getHtblColNameValue().get(clusteringKey)) > value) {
//                right = mid - 1;
//            } else
//                left = mid + 1;
//        }
//        return -1;
//    }
//public static int binarySearch(Vector <Tuple> tuples, int x)
//{
//    while (l <= r) {
//        int mid = (l + r) / 2;
//
//        // If the element is present at the
//        // middle itself
//        if (arr[mid] == x) {
//            return mid;
//
//            // If element is smaller than mid, then
//            // it can only be present in left subarray
//            // so we decrease our r pointer to mid - 1
//        } else if (arr[mid] > x) {
//            r = mid - 1;
//
//            // Else the element can only be present
//            // in right subarray
//            // so we increase our l pointer to mid + 1
//        } else {
//            l = mid + 1;
//        }
//    }
//
//    // We reach here when element is not present
//    //  in array
//    return -1;
//}

    public static String checkForColumnType(String strTableName, String strColName) throws IOException {
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null) {
            String[] tableData = line.split(splitBy);
            if (tableData[0].equals(strTableName) && strColName.equals(tableData[1])) {
                return tableData[2];
            }
        }
        return null;
    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue   )  throws DBAppException, ClassNotFoundException, IOException{
        if(!checkTableExists(strTableName))
            throw new DBAppException("Table does not exist");

        Table currTable = DeserializeTable(strTableName);
        Set<String> names = htblColNameValue.keySet(); // 34an tgeb el keys
//                    Vector<String> keys = new Vector<>();
        for (String columnName : names) {
            if(!(checkColumnExists(strTableName,columnName)))
                throw new DBAppException("Some columns do not exist in this table");
        }
        String line = "";
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
        while ((line = br.readLine()) != null){
            String[] tableData = line.split(splitBy);
            if(tableData[0].equals(strTableName)&& names.contains(tableData[1])){
                String type=tableData[2];
                switch (type) {
                    case "java.lang.Integer" -> {
                        if (!(htblColNameValue.get(tableData[1]) instanceof Integer))
                            throw new DBAppException("Wrong data type");
                    }
                    case "java.lang.double" -> {
                        System.out.println(tableData[1]);
                        if (!(htblColNameValue.get(tableData[1]) instanceof Double))
                            throw new DBAppException("Wrong data type");
                    }
                    case "java.lang.String" -> {
                        if (!(htblColNameValue.get(tableData[1]) instanceof String))
                            throw new DBAppException("Wrong data type");
                    }
                }
            }

        }

        Comparable value = null;
        int typeFlag = 0;

        BufferedReader br1 = new BufferedReader(new FileReader("metadata.csv"));
        while (( line = br1.readLine()) != null){
            String[] tableData = line.split(splitBy);
            if(tableData[0].equals(strTableName)&&tableData[3].equals("True")){
                String type=tableData[2];
                switch (type) {
                    case "java.lang.Integer" -> {
                        value = Integer.parseInt(strClusteringKeyValue);
                        typeFlag = 0;
                        break;
                    }
                    case "java.lang.double" -> {
                        value = Double.parseDouble(strClusteringKeyValue);
                        typeFlag = 1;
                        break;
                    }
                    case "java.lang.String" -> {
                        value = strClusteringKeyValue;
                        typeFlag = 2;
                        break;
                    }
                }
            }

        }
//        if(htblColNameValue.get(currTable.getStrClusteringKeyColum())==null)
//            throw new DBAppException("Missing clustering key");
        String clustringKey = currTable.getStrClusteringKeyColum();
        System.out.println("clustring key cloumn = "+clustringKey);
        Vector<String> pagesOfcurrTable = currTable.getPages() ;//get pages of table
        boolean flag = false ;
        if(chackForIndex(strTableName,currTable.getStrClusteringKeyColum())){
            String indexName = getIndexName(strTableName,currTable.getStrClusteringKeyColum());
            BTree index = indexDeserialization(indexName);
//            System.out.println("my key __________________" + htblColNameValue.get(currTable.getStrClusteringKeyColum()) );
            System.out.println("my value __________________" + value );
            ArrayList <Integer> pages =(ArrayList<Integer>) index.search((Comparable) value);
            System.out.println("Page to search in = "+pages);
            int pageNumber = pages.get(0)-1;
            System.out.println("pageNumber = "+pageNumber);
//            for(int i =1 ; i<pages.size()+1;i++){
                System.out.println("page number = " + pageNumber);
                Page pageCurrentlySearchingIn = currTable.pageDeserialization(pagesOfcurrTable.get(pageNumber));
                System.out.println("pageCurrentlySearchingIn = " + pageCurrentlySearchingIn);
                for (int j = 0; j < pageCurrentlySearchingIn.getTuples().size(); j++) {
                    Tuple currTuple = pageCurrentlySearchingIn.getTuples().get(j);
                    System.out.println("tuple = " + pageCurrentlySearchingIn.getTuples().get(j));
                    System.out.println("Compare kda: " + currTuple.getHtblColNameValue().get(clustringKey) + " we da " + strClusteringKeyValue);
                    if (currTuple.getHtblColNameValue().get(clustringKey).toString().equals(strClusteringKeyValue)) {
                        Set<String> columnNames = htblColNameValue.keySet(); // 34an tgeb el keys
                        System.out.println("Ana gowa el loop!!");
//                    Vector<String> keys = new Vector<>();
                        for (String columnName : columnNames) {
                            currTuple.getHtblColNameValue().put(columnName, htblColNameValue.get(columnName));
                            System.out.println(columnName);
                            // bos hena ya 3badoooooooooooo!!!!!!!!!!
                        }
//                    currTuple = new Tuple(htblColNameValue);
                        //currTuble.setHtblColNameValue(htblColNameValue) ;
//                    String column = htblColNameValue.get;
//                    htblColNameValue.entrySet();
//                    String columnName = entry.getKey();
//                    Object value = entry.getValue();
                        flag = true;
                        break;
                    }

                }
                currTable.pageSerialization(pageCurrentlySearchingIn);
//                if (flag)
//                    break;
//            }
        }
        else {
            for (int i = 0; i < pagesOfcurrTable.size(); i++) {
                Page pageCurrentlySearchingIn = currTable.pageDeserialization(pagesOfcurrTable.get(i));
                System.out.println("pageCurrentlySearchingIn = " + pageCurrentlySearchingIn);
                for (int j = 0; j < pageCurrentlySearchingIn.getTuples().size(); j++) {
                    Tuple currTuple = pageCurrentlySearchingIn.getTuples().get(j);
                    System.out.println("tuple = " + pageCurrentlySearchingIn.getTuples().get(j));
                    System.out.println("Compare kda: " + currTuple.getHtblColNameValue().get(clustringKey) + " we da " + strClusteringKeyValue);
                    if (currTuple.getHtblColNameValue().get(clustringKey).toString().equals(strClusteringKeyValue)) {
                        Set<String> columnNames = htblColNameValue.keySet(); // 34an tgeb el keys
                        System.out.println("Ana gowa el loop!!");
//                    Vector<String> keys = new Vector<>();
                        for (String columnName : columnNames) {
                            currTuple.getHtblColNameValue().put(columnName, htblColNameValue.get(columnName));
                            System.out.println(columnName);
                            // bos hena ya 3badoooooooooooo!!!!!!!!!!
                        }
//                    currTuple = new Tuple(htblColNameValue);
                        //currTuble.setHtblColNameValue(htblColNameValue) ;
//                    String column = htblColNameValue.get;
//                    htblColNameValue.entrySet();
//                    String columnName = entry.getKey();
//                    Object value = entry.getValue();
                        flag = true;
                        break;
                    }

                }
                currTable.pageSerialization(pageCurrentlySearchingIn);
                if (flag)
                    break;
            }
        }
        String line2 = "";
        String splitBy2 = ",";
        BufferedReader br2 = new BufferedReader(new FileReader("metadata.csv"));
        while ((line2 = br2.readLine()) != null){
            String[] tableData = line2.split(splitBy2);
            if(tableData[0].equals(strTableName)){
                String colName = tableData[1];
                if(chackForIndex(strTableName,colName)){
                    String indexName = getIndexName(strTableName,colName);
                    createIndex(strTableName,colName,indexName);
//                        BTree index = indexDeserialization(indexName);
//                        Comparable key = (Comparable) htblColNameValue.get(colName);
//                        ArrayList <Integer>listOfPages;
//                        if (index.search(key) == null) {
//                            listOfPages = new ArrayList<>();
//                            listOfPages.add(pageNumber);
//                            index.insert(key,listOfPages);
//                        }else{
//                            listOfPages = (ArrayList<Integer>) index.search(key);
//                            boolean alreadyExists = false;
////                            for(int j=0;j<listOfPages.size();j++){
////                                if(listOfPages.get(j).equals()){}
////                            }
//                                if (listOfPages.contains()) {
//                                }
//                        }
                }
            }
        }
//        throw new DBAppException("not implemented yet");
    }



    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        //delete tuple
        try {

            if (!checkTableExists(strTableName))
                throw new DBAppException("Table does not exist");
            //Table currTable = DeserializeTable(strTableName);
            Set<String> names = htblColNameValue.keySet(); // 34an tgeb el keys
//                    Vector<String> keys = new Vector<>();
            for (String columnName : names) {
                if (!(checkColumnExists(strTableName, columnName)))
                    throw new DBAppException("Some columns do not exist in this table");
            }
            String line = "";
            String splitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
            while ((line = br.readLine()) != null) {
                String[] tableData = line.split(splitBy);
                if (tableData[0].equals(strTableName) && names.contains(tableData[1])) {
                    String type = tableData[2];
                    switch (type) {
                        case "java.lang.Integer" -> {
                            if (!(htblColNameValue.get(tableData[1]) instanceof Integer))
                                throw new DBAppException("Wrong data type");
                        }
                        case "java.lang.double" -> {
//                            System.out.println(tableData[1]);
                            if (!(htblColNameValue.get(tableData[1]) instanceof Double))
                                throw new DBAppException("Wrong data type");
                        }
                        case "java.lang.String" -> {
                            if (!(htblColNameValue.get(tableData[1]) instanceof String))
                                throw new DBAppException("Wrong data type");
                        }
                    }
                }
            }

            //delete tuple


            Table table = DeserializeTable((strTableName));
            // Comparable key =(Comparable)htblColNameValue.get(table.getStrClusteringKeyColum());
            boolean flag =false;
            boolean pageFlag=false;
            //Enumeration<String> keysEnum = htblColNameValue.keys();
            //        for (String key : hashtable.keySet()) {
            //search in each page
            // el column names ahy ya negm
            Set<String> columns = htblColNameValue.keySet(); // 34an tgeb el keys
//                    Vector<String> keys = new Vector<>();
            boolean indexExists=false,first = true;
            ArrayList<Integer> pagesToSearch = null;
            for (String columnName : columns) {
                if (chackForIndex(strTableName, columnName)) {
                    indexExists = true;
                    String indexName = getIndexName(strTableName, columnName);
                    BTree index = indexDeserialization(indexName);
                    if (first) {
                        pagesToSearch = (ArrayList<Integer>) index.search((Comparable) htblColNameValue.get(columnName));
                        first = false;
                    } else {
                        ArrayList<Integer> temp = (ArrayList<Integer>) index.search((Comparable) htblColNameValue.get(columnName));
                        pagesToSearch = findIntersection(pagesToSearch, temp);
                    }
                }
            }
            if(indexExists && pagesToSearch!=null) {
                for (int pagen = 0; pagen < pagesToSearch.size(); pagen++) {
                    String pageName = table.getPages().get(pagesToSearch.get(pagen));
                    Page p = table.pageDeserialization(pageName);
                    for (int i = 0; i < p.getTuples().size(); i++) {
                        Tuple t = p.getTuples().get(i);
                        //check each value of the required
                        for (String key : htblColNameValue.keySet()) {

                            //Object key1 =(Object) key;
                            Comparable value = (Comparable) htblColNameValue.get(key);
                            if (!(value.compareTo((Comparable) (t.getHtblColNameValue().get(key))) == 0)) {
                                flag = true;
                                break;
                            }


                        }
                        if (!flag) {
                            p.getTuples().remove(t);
                            i--;
                        }
                        flag = false;


                    }

                    table.pageSerialization(p);

                }
            }

            else {
                for (int pagen = 0; pagen < table.getPages().size(); pagen++) {
                    String pageName = table.getPages().get(pagen);
                    Page p = table.pageDeserialization(pageName);

                    //search each tuple
                    for (int i = 0; i < p.getTuples().size(); i++) {
                        Tuple t = p.getTuples().get(i);
                        //check each value of the required
                        for (String key : htblColNameValue.keySet()) {

                            //Object key1 =(Object) key;
                            Comparable value = (Comparable) htblColNameValue.get(key);
                            if (!(value.compareTo((Comparable) (t.getHtblColNameValue().get(key))) == 0)) {
                                flag = true;
                                break;
                            }


                        }
                        if (!flag) {
                            p.getTuples().remove(t);
                            i--;
                        }
                        flag = false;


                    }

                    table.pageSerialization(p);

                }
            }
            Vector<Page> newPages = new Vector<>();
            for (int i = 0; i < table.getPages().size(); i++) {
                Page newPage = new Page(table.getPages().get(i));
                newPages.add(newPage);
            }
            int j = 0;
            for (int pagen = 0; pagen < table.getPages().size(); pagen++) {
                String pageName = table.getPages().get(pagen);
                Page p = table.pageDeserialization(pageName);
                if (p.getTuples().size() != 0) {
                    newPages.get(j).setTuples(p.getTuples());
                    j++;
//                    System.out.println("i = "+ pagen + "  " + "j = "+j);
                }
            }
            for (int pagen = 0; pagen < table.getPages().size(); pagen++) {
//                System.out.println("bmsa7 abohom!!");
                String pageName = table.getPages().get(pagen);
                File fileToDelete = new File("pages\\"+pageName+".ser");
                fileToDelete.delete();
            }
            table.getPages().clear();
            for (int pagen = 0; pagen < newPages.size(); pagen++) {
                if (newPages.get(pagen).getTuples().size() != 0) {
//                    System.out.println("b7ottohom tany");
                    Page newPage = newPages.get(pagen);
                    table.getPages().add(newPage.getName());
                    table.pageSerialization(newPage);
                }
            }

            FileOutputStream fileOut = new FileOutputStream("Tables\\" + strTableName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();

            System.out.println("Serialized data is saved in " + strTableName + ".ser");

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String line2 = "";
        String splitBy2 = ",";
        BufferedReader br2 = new BufferedReader(new FileReader("metadata.csv"));
        while ((line2 = br2.readLine()) != null){
            String[] tableData = line2.split(splitBy2);
            if(tableData[0].equals(strTableName)){
                String colName = tableData[1];
                if(chackForIndex(strTableName,colName)){
                    String indexName = getIndexName(strTableName,colName);
                    createIndex(strTableName,colName,indexName);
//                        BTree index = indexDeserialization(indexName);
//                        Comparable key = (Comparable) htblColNameValue.get(colName);
//                        ArrayList <Integer>listOfPages;
//                        if (index.search(key) == null) {
//                            listOfPages = new ArrayList<>();
//                            listOfPages.add(pageNumber);
//                            index.insert(key,listOfPages);
//                        }else{
//                            listOfPages = (ArrayList<Integer>) index.search(key);
//                            boolean alreadyExists = false;
////                            for(int j=0;j<listOfPages.size();j++){
////                                if(listOfPages.get(j).equals()){}
////                            }
//                                if (listOfPages.contains()) {
//                                }
//                        }
                }
            }
        }
//        throw new DBAppException("not implemented yet");
}


    public static ArrayList<Integer> findIntersection(ArrayList<Integer> list1, ArrayList<Integer> list2) {
        Set<Integer> set1 = new HashSet<>(list1);
        Set<Integer> set2 = new HashSet<>(list2);

        // Create a new list to store the intersection
        ArrayList<Integer> intersection = new ArrayList<Integer>();

        // Iterate over set1 and check if each element is in set2
        for (Integer element : set1) {
            if (set2.contains(element)) {
                intersection.add(element);
            }
        }

        return intersection;
    }


    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, ClassNotFoundException, IOException {


        ArrayList<Tuple> temp = new ArrayList<Tuple>();
        ArrayList<ArrayList<Tuple>> arrayListsOfAns = new ArrayList<ArrayList<Tuple>>();
        for (int i = 0; i < arrSQLTerms.length; i++) {
            String tableName = arrSQLTerms[i]._strTableName;
            Object strClusteringKeyValue = arrSQLTerms[i]._objValue;
            String strColumnName = arrSQLTerms[i]._strColumnName;
            String operator = arrSQLTerms[i]._strOperator;


            if (!checkTableExists(tableName)) {
                throw new DBAppException("Table does not exist");
            }


//		        Table currTable = DeserializeTable(tableName);
// malosh lazma sa7		    Set<String> names = htblColNameValue.keySet(); // 34an tgeb el keys
//		                    Vector<String> keys = new Vector<>();
            if (!(checkColumnExists(tableName, strColumnName))) {    //updated removed for loop
                throw new DBAppException("Some columns do not exist in this table");
            }


            String line = "";
            String splitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
            while ((line = br.readLine()) != null) {
                String[] tableData = line.split(splitBy);
                if (tableData[0].equals(tableName) && tableData[1].equals(strColumnName)) {  //updated
                    String type = tableData[2];
                    switch (type) {
                        case "java.lang.Integer" -> {
                            if (!(strClusteringKeyValue instanceof Integer)) {
                                throw new DBAppException("Wrong data type");
                            }

                        }
                        case "java.lang.double" -> {
                            //System.out.println(tableData[1]);
                            if (!(strClusteringKeyValue instanceof Double)) {
                                throw new DBAppException("Wrong data type");
                            }
                        }
                        case "java.lang.String" -> {
                            if (!(strClusteringKeyValue instanceof String)) {
                                throw new DBAppException("Wrong data type");
                            }
                        }
                    }
                }

            }

            ArrayList<Tuple> neededRecords = new ArrayList<Tuple>();

            if(chackForIndex(tableName, strColumnName)) {
                System.out.println(chackForIndex(tableName, strColumnName));
                System.out.println("I am inside indexed");
                String indexName = getIndexName(tableName, strColumnName);
                BTree Index = indexDeserialization(indexName);
                ArrayList<Integer> pagesHavingNeededRecords = (ArrayList<Integer>)Index.search((Comparable) strClusteringKeyValue);
                Table currTable = DeserializeTable(tableName);
                for (int j = 0; j < pagesHavingNeededRecords.size(); j++) {
                    String pageToSearchIn = currTable.getPages().get(pagesHavingNeededRecords.get(j)-1);
                    Page pageCurrentlySearchingIn = currTable.pageDeserialization(pageToSearchIn) ;

                    for (int  k= 0; k < pageCurrentlySearchingIn.getTuples().size(); k++) {
                        Tuple currTuple = pageCurrentlySearchingIn.getTuples().get(k);
                        if (getOperatorFunction((Comparable) currTuple.getHtblColNameValue().get(strColumnName), (Comparable) strClusteringKeyValue, operator)) {
                            neededRecords.add(currTuple);
                        }

                    }
                    currTable.pageSerialization(pageCurrentlySearchingIn);

                }
            }


            else {
                System.out.println("I am inside non-indexed");
                neededRecords = search(tableName, strClusteringKeyValue, strColumnName, operator);
            }

            arrayListsOfAns.add(neededRecords);
            // System.out.println(neededRecords.toString());
        }
        temp.clear();
        for (int i = 0; i < strarrOperators.length && arrayListsOfAns.size() > 1; i++) {
            if (strarrOperators[i].equals("AND")) {

                ArrayList<Tuple> answer = strarrOperators((ArrayList<Tuple>) arrayListsOfAns.get(i), (ArrayList<Tuple>) arrayListsOfAns.get(i + 1), strarrOperators[i]);
                strarrOperators = removeElement(strarrOperators, i);
                System.out.println("size:" + arrayListsOfAns.size());
                arrayListsOfAns.remove(i + 1);
                arrayListsOfAns.remove(i);
                arrayListsOfAns.add(i, answer);
                System.out.println("end of for And loop");
            }
        }
        System.out.println(arrayListsOfAns.size());
        for (int i = 0; i < strarrOperators.length && arrayListsOfAns.size() > 1; i++) {
            if (strarrOperators[i].equals("OR")) {
                ArrayList<Tuple> answer = strarrOperators((ArrayList<Tuple>) arrayListsOfAns.get(i), (ArrayList<Tuple>) arrayListsOfAns.get(i + 1), strarrOperators[i]);
                strarrOperators = removeElement(strarrOperators, i);
                arrayListsOfAns.remove(i + 1);
                arrayListsOfAns.remove(i);
                arrayListsOfAns.add(i, answer);
            }
        }
        for (int i = 0; i < strarrOperators.length && arrayListsOfAns.size() > 1; i++) {
            if (strarrOperators[i].equals("XOR")) {
                ArrayList<Tuple> answer = strarrOperators((ArrayList<Tuple>) arrayListsOfAns.get(i), (ArrayList<Tuple>) arrayListsOfAns.get(i + 1), strarrOperators[i]);
                strarrOperators = removeElement(strarrOperators, i);
                arrayListsOfAns.remove(i + 1);
                arrayListsOfAns.remove(i);
                arrayListsOfAns.add(i, answer);
            }
        }
        System.out.println("the answer size" + arrayListsOfAns.size());
        for (int i = 0; i < arrayListsOfAns.size(); i++) {
            ArrayList<Tuple> curr = arrayListsOfAns.get(i);
            for (int j = 0; j < curr.size(); j++) {
                temp.add(curr.get(j));
            }
        }

        Iterator I = temp.iterator();
        return I;
    }

    public static String[] removeElement(String[] arr, int index) {

        String[] answer = new String[arr.length - 1];
        for (int i = 0; i < index; i++) {
            answer[i] = arr[i];
        }
        for (int i = index; i < arr.length - 1; i++) {
            answer[i] = arr[i + 1];
        }

        return answer;
    }

    public static ArrayList<Tuple> strarrOperators(ArrayList<Tuple> first, ArrayList<Tuple> second,
                                                   String strarrOperators) {
        ArrayList<Tuple> temp = new ArrayList<Tuple>();
        switch (strarrOperators) {
            case "OR":
                for (int i = 0; i < first.size(); i++) {
                    Hashtable<String, Object> firstValue = first.get(i).getHtblColNameValue();
                    for (int j = 0; j < second.size(); j++) {
                        Hashtable<String, Object> secondValue = second.get(j).getHtblColNameValue();
                        if (firstValue.equals(secondValue)) {
                            temp.add(first.get(i));
                            first.remove(i);
                            i--;
                            second.remove(j);
                            break;
                        }
                    }
                }
                for (int i = 0; i < first.size(); i++) {
                    temp.add(first.get(i));
                }
                for (int i = 0; i < second.size(); i++) {
                    temp.add(second.get(i));
                }
                return temp;

            case "AND":
                System.out.println("inside AND");
                for (int i = 0; i < first.size(); i++) {
                    Hashtable<String, Object> firstValue = first.get(i).getHtblColNameValue();
                    for (int j = 0; j < second.size(); j++) {
                        Hashtable<String, Object> secondValue = second.get(j).getHtblColNameValue();
                        if (firstValue.equals(secondValue)) {
                            temp.add(first.get(i));
                            break;
                        }
                    }
                }
                return temp;
            case "XOR":
                for (int i = 0; i < first.size(); i++) {
                    Hashtable<String, Object> firstValue = first.get(i).getHtblColNameValue();
                    for (int j = 0; j < second.size(); j++) {
                        Hashtable<String, Object> secondValue = second.get(j).getHtblColNameValue();
                        if (firstValue.equals(secondValue)) {
                            first.remove(i);
                            i--;
                            second.remove(j);
                            break;
                        }
                    }
                }
                for (int i = 0; i < first.size(); i++) {
                    temp.add(first.get(i));
                }
                for (int i = 0; i < second.size(); i++) {
                    temp.add(second.get(i));
                }
                return temp;
            default:
                return temp;
        }
    }


    public static boolean getOperatorFunction(Comparable value1, Comparable value2, String operator) {
        switch (operator) {
            case ">":
                if (value1.compareTo(value2) > 0)
                    return true;
                else
                    return false;
            case ">=":
                if (value1.compareTo(value2) >= 0)
                    return true;
                else
                    return false;
            case "<":
//                System.out.println("ana gowa ya 3rs");
                if (value1.compareTo(value2) < 0)
                    return true;
                else
                    return false;
            case "<=":
                if (value1.compareTo(value2) <= 0)
                    return true;
                else
                    return false;
            case "!=":
                if (value1.compareTo(value2) != 0)
                    return true;
                else
                    return false;
            case "=":
                if (value1.compareTo(value2) == 0)
                    return true;
                else
                    return false;
            default:
                return false;
        }
    }

    public static ArrayList<Tuple> search(String strTableName, Object strClusteringKeyValue,
                                          String strColumnName, String operator)
            throws ClassNotFoundException, IOException {

        Table currTable = DeserializeTable(strTableName);
        Vector<String> pagesOfcurrTable = currTable.getPages();
        ArrayList<Tuple> ans = new ArrayList<Tuple>();


        for (int i = 0; i < pagesOfcurrTable.size(); i++) {

            Page pageCurrentlySearchingIn = currTable.pageDeserialization(pagesOfcurrTable.get(i));
            //System.out.println("pageCurrentlySearchingIn = " + pageCurrentlySearchingIn);

            for (int j = 0; j < pageCurrentlySearchingIn.getTuples().size(); j++) {
                Tuple currTuple = pageCurrentlySearchingIn.getTuples().get(j);
                // System.out.println("tuple = " + pageCurrentlySearchingIn.getTuples().get(j));
                //System.out.println("Compare kda: " + currTuple.getHtblColNameValue().get(clustringKey) + " we da "
                //		+ strClusteringKeyValue);


                if (getOperatorFunction((Comparable) currTuple.getHtblColNameValue().get(strColumnName), (Comparable) strClusteringKeyValue, operator)) {
                    ans.add(currTuple);
                }

            }
            currTable.pageSerialization(pageCurrentlySearchingIn);

        }return ans;

    }

    public static void main(String[]args) throws DBAppException, IOException, ClassNotFoundException {
        String strTableName = "LEO";
        DBApp	dbApp = new DBApp( );
////
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        dbApp.createTable( strTableName, "id", htblColNameType );
//////
//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer(100));
//        htblColNameValue.put("name", new String("Ali Hossam" ) );
//        htblColNameValue.put("gpa", new Double( 0.96 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
////        htblColNameValue.clear( );
////        htblColNameValue.put("id", new Integer( 34 ));
////        htblColNameValue.put("name", new String("Ziad Maghraby" ) );
////        htblColNameValue.put("gpa", new Double( 0.95 ) );
////        dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 201 ));
//        htblColNameValue.put("name", new String("Negm4" ) );
//        htblColNameValue.put("gpa", new Double( 1.25 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
////
//        htblColNameValue.clear( );
//        htblColNameValue.put("id", new Integer( 80 ));
//        htblColNameValue.put("name", new String("Rayes 7efny" ) );
//        htblColNameValue.put("gpa", new Double( 0.5 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
//        for (int i = 1; i < 60 ; i++){
//            htblColNameValue.clear();
//            htblColNameValue.put("id", new Integer(i));
//            htblColNameValue.put("name", new String("Negm"+i));
//            htblColNameValue.put("gpa", new Double(i + 0.5));
//            dbApp.insertIntoTable(strTableName, htblColNameValue);
//        }

//        dbApp.createIndex( strTableName, "name","nameIndex" );
//                dbApp.createIndex(strTableName,"gpa","gpaIndex");

        Hashtable<String,Object> h = new Hashtable<>();
//        h.put("name", "Negm6");
        h.put("gpa",0.22222);
        dbApp.updateTable(strTableName,"10",h);

//        dbApp.deleteFromTable(strTableName,h);
        Table t = DeserializeTable(strTableName);
        System.out.println(t.getPages());
        for (String p: t.getPages()){
            Page pa = t.pageDeserialization(p);
            System.out.println(pa+"\n");
        }
//        dbApp.createIndex(strTableName,"id","IdIndex");
//        Table table = DeserializeTable("ohhh");
//        Page p = table.pageDeserialization("ohhh2");
//        System.out.println(binarySearch("ohhh",p,26));

////        h.put("")

//        Table r = DeserializeTable(strTableName);
//        System.out.println(r.getPages());
//        for (String p: r.getPages()){
//            Page pa = r.pageDeserialization(p);
//            System.out.println(pa+"\n");
//        }

//        System.out.println(t.getPages());
//        for (String p: t.getPages()){
//            Page pa = t.pageDeserialization(p);
//            System.out.println(pa+"\n");
//        }
//        Table g = DeserializeTable(strTableName);
//        for (String p: g.getPages()){
//            Page pa = t.pageDeserialization(p);
//            System.out.println(pa+"\n");
//        }
//        try {
//            dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
//            page1.insertTuple(new Tuple("mwada", "07775000", 20));
//            page1.insertTuple(new Tuple("Batta", "meow", 22));
//            page1.insertTuple(new Tuple("Emad", "55-23897", 21));
//            pageSerialization(page1);
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        arrSQLTerms[1] = new SQLTerm();
//        arrSQLTerms[1]._strTableName = strTableName;
//        arrSQLTerms[1]._strColumnName= "name";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue = "Negm2";
//        String[]strarrOperators = new String[2];
//        strarrOperators[1] = "AND";
//        strarrOperators[0] = "OR";
//        arrSQLTerms[0] = new SQLTerm();
//        arrSQLTerms[0]._strTableName = strTableName;
//        arrSQLTerms[0]._strColumnName= "id";
//        arrSQLTerms[0]._strOperator = "<=";
//        arrSQLTerms[0]._objValue = new Integer(20);
//        arrSQLTerms[2] = new SQLTerm();
//        arrSQLTerms[2]._strTableName = strTableName;
//        arrSQLTerms[2]._strColumnName= "gpa";
//        arrSQLTerms[2]._strOperator = ">";
//        arrSQLTerms[2]._objValue = new Double(0.80);
//        int count = 1 ;
//        Iterator iterator = dbApp.selectFromTable(arrSQLTerms,strarrOperators);
//        System.out.println("the select iterator");
//        while (iterator.hasNext()) {
//            Tuple tuple = (Tuple) iterator.next();
//            // Perform operations on the tuple
//            // For example, print its attributes
//            System.out.println(tuple.toString());
//            System.out.println("end of Tuple "+count++);
//
//            }
    }

}