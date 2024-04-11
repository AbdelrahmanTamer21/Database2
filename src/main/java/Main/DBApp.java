package Main;
/** * @author Wael Abouelsaadat */

import Exception.DBAppException;
import Utilities.Serializer;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import sql.SQLTerm;
import sql.parser.SQLParser;

import java.io.*;
import java.util.*;


public class DBApp {

	public static int pageSize = readConfig("MaximumRowsCountinPage");
	public static int nodeOrder = readConfig("TreeNodeOrder");

	private final HashSet<String> myTables;

	public DBApp( ){
		this.myTables = new HashSet<>();
		init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		File pagesDir = new File("Pages");
		pagesDir.mkdir();
		File tablesDir = new File("Tables");
		tablesDir.mkdir();
		File metadata = new File("metadata.csv");
		try{
			metadata.delete();
			metadata.createNewFile();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	//Read a property from the .config File
	public static int readConfig(String property){
		Properties properties = new Properties();
		try {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			InputStream fileInputStream = classloader.getResourceAsStream("DBApp.config");
			properties.load(fileInputStream);
			assert fileInputStream != null;
			fileInputStream.close();

			return Integer.parseInt(properties.getProperty(property));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return 2;
	}


	// Following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException {
		validateTable(strTableName,strClusteringKeyColumn,htblColNameType);
		// first create a file object for file placed at location specified by filepath
		// Create the metadata for the table
		File file = new File("metadata.csv");
		try{
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter outputFile = new FileWriter(file,true);

			//create CSVWriter with ',' as separator
			CSVWriter writer = new CSVWriter(outputFile, ',',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);

			// create a List which contains a String array
			List<String[]> data = new ArrayList<>();
			Enumeration<String> e = htblColNameType.keys();
			while (e.hasMoreElements()) {

				// Getting the key of a particular entry
				String key = e.nextElement();
				String isPrimaryKey = String.valueOf(strClusteringKeyColumn.equals(key));
				//Column type
				String type = htblColNameType.get(key);

				//Check type is within bounds
				if(!Objects.equals(type, "java.lang.Integer")
						&& !Objects.equals(type, "java.lang.String")
						&& !Objects.equals(type, "java.lang.Double"))
				{
					throw new DBAppException("Data type not supported");
				}

				//Order is -> Table Name, Column Name, Column Type, IsClusteringKey, Index Name, Index Type
				data.add(new String[] {strTableName, key, type, isPrimaryKey, "null", "null"});
			}
			Collections.reverse(data);
			//Make the primary key the first value
			Comparator<String[]> comparator = (arr1, arr2) -> {
				boolean value1 = Boolean.parseBoolean(arr1[3]);
				boolean value2 = Boolean.parseBoolean(arr2[3]);

				// Put "true" values before "false" values
				return Boolean.compare(value2, value1); // Reverse order to put "true" before "false"
			};
			data.sort(comparator);
			LinkedHashMap<String, String> attr = new LinkedHashMap<>();
			for (String[] row : data) {
				attr.put(row[1],row[2]);
			}
			writer.writeAll(data);
			Table table = new Table(strTableName,strClusteringKeyColumn,attr);
			// closing writer connection
			writer.close();

			myTables.add(strTableName);
			Serializer.serializeTable(table,strTableName);
		}catch (IOException e){
			e.printStackTrace();
		}
	}


	private void validateTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
		if(myTables.contains(strTableName)){
			throw new DBAppException("Table name already exists");
		}
		if(strClusteringKeyColumn == null || !htblColNameType.containsKey(strClusteringKeyColumn)){
			throw new DBAppException("Clustering key is invalid");
		}
		if(checkTableMetadataExists(strTableName)){
			throw new DBAppException("Table already exists");
		}
	}

	private boolean checkTableMetadataExists(String strTableName){
		File file = new File("metadata.csv");
		try{
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] values = line.split(",");
				if(values[0].equals(strTableName)){
					return true;
				}
			}
			scanner.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return false;
	}
	private Table checkTableExits(String strTableName) throws DBAppException {
		if (!myTables.contains(strTableName)){
			throw new DBAppException("Table does not exist");
		}else {
			Table table = Serializer.deserializeTable(strTableName);
			ArrayList<String[]> row = new ArrayList<>();
			File file = new File("metadata.csv");
			try(Scanner scanner = new Scanner(file)){
				boolean hasNextLine = scanner.hasNextLine();
				String line = scanner.nextLine();
				while (hasNextLine) {
					String[] values = line.split(",");
					String tableName = values[0];
					// If the tableName doesn't match, skip this line
					if (!tableName.equals(strTableName)) {
						hasNextLine = scanner.hasNextLine();
						line = scanner.nextLine();
						continue;
					}
					row.add(values);
					hasNextLine = scanner.hasNextLine();
					if (!hasNextLine)
						break;
					line = scanner.nextLine();
					// If the next line's tableName doesn't match, break the loop
					if (!line.startsWith(strTableName))
						break;
				}
				scanner.close();
				assert table != null;
				if(table.getAttributes().size()!=row.size()){
					throw new DBAppException("Table metadata is corrupted");
				}else {
					for (String[] values : row) {
						boolean isPrimaryKey = Boolean.parseBoolean(values[3]);
						String colName = values[1];
						String colType = values[2];
						String indexName = values[4];
						if ((isPrimaryKey && !table.getPrimaryKey().equals(colName))
								|| !table.getAttributes().containsKey(colName)
								|| !table.getAttributes().get(colName).equals(colType)
								|| (!Objects.equals(indexName, "null") && !table.getIndexNames().contains(indexName))) {
							throw new DBAppException("Table attributes/primaryKey/indexNames does not match the csv file");
						}
					}
				}
			}catch (IOException e){
				e.printStackTrace();
			}
			return table;
		}
	}

	// the following method creates a B+tree index
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException {

		checkTableExits(strTableName);
		Table table = Serializer.deserializeTable(strTableName);
		assert table != null;
		table.createIndex(strColName,strIndexName,false);
		Serializer.serializeTable(table,strTableName);
		File file = new File("metadata.csv");
		try{
			FileReader inputFile = new FileReader(file);
			// Read existing file
			CSVReader reader = new CSVReader(inputFile);
			List<String[]> csvBody = reader.readAll();
			// get CSV row column and replace with by using row and column
			for (String[] strArray : csvBody) {
				if (strArray[0].equals(strTableName) && strArray[1].equals(strColName)) {
					strArray[4] = strIndexName;
					strArray[5] = "B+Tree";
					break;
				}
			}
			reader.close();

			FileWriter outputFile = new FileWriter(file);
			// Write to CSV file which is open
			CSVWriter writer = new CSVWriter(outputFile, ',',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);
			writer.writeAll(csvBody);
			writer.close();

		}catch (Exception e){
			e.printStackTrace();
		}
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException {
		Table table = checkTableExits(strTableName);
		assert table != null;
		table.insertTuple(htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include a clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException {

		Table table = checkTableExits(strTableName);
		assert table != null;
		table.updateTuple(strClusteringKeyValue,htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}


	// The following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException {
		Table table = checkTableExits(strTableName);
		assert table != null;
		table.deleteTuples(htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException {
		if(arrSQLTerms.length!=strarrOperators.length+1){
			throw new DBAppException("Num of operators must be = SQLTerms -1");
		}
		if(arrSQLTerms.length>=1) {
			String tableName = arrSQLTerms[0]._strTableName;
			Table table = checkTableExits(tableName);
			assert table != null;
			//Edge case checks
			for (SQLTerm arrSQLTerm : arrSQLTerms) {
				if (!Objects.equals(arrSQLTerm._strTableName, tableName)) {
					throw new DBAppException("One of the SQLTerms isn't on the same table");
				}
				if (!table.getAttributes().containsKey(arrSQLTerm._strColumnName)) {
					throw new DBAppException("The Table doesn't contain a " + arrSQLTerm._strColumnName + " column");
				}
				if (!arrSQLTerm._objValue.getClass().getName().equals(table.getAttributes().get(arrSQLTerm._strColumnName))) {
					throw new DBAppException("Class of the object for the operation doesn't match the column class");
				}
				if (!Arrays.asList("<", "<=", ">", ">=", "!=", "=").contains(arrSQLTerm._strOperator)) {
					throw new DBAppException("The only supported operators are <,<=,>,>=,!=,=");
				}
			}
			for (String operator : strarrOperators){
				if (!Arrays.asList("AND","OR","XOR").contains(operator.toUpperCase())) {
					throw new DBAppException("The only supported array operators are AND,OR,XOR");
				}
			}
			return table.selectFromTable(arrSQLTerms,strarrOperators);
		}
		return null;
	}

	public HashSet<String> getMyTables(){
		return myTables;
	}

	public void deleteTable(String strTableName) throws DBAppException {
		if (!myTables.contains(strTableName)){
			throw new DBAppException("Table does not exist");
		}
		Table table = Serializer.deserializeTable(strTableName);
		assert table != null;
		File pagesDir = new File("Pages/"+strTableName);
		File[] files = pagesDir.listFiles();
		for (File file : files) {
			file.delete();
		}
		pagesDir.delete();
		File tableFile = new File("Tables/"+strTableName+".ser");
		tableFile.delete();
		myTables.remove(strTableName);
	}

	public Iterator parseSQL(StringBuffer stringBuffer){
		SQLParser sqlParser = new SQLParser(this);
		Iterator resultSet = sqlParser.parseSQL(stringBuffer);
		return resultSet;
	}

	public static void main( String[] args ){
	
	try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );

			Hashtable<String,String> htblColNameType = new Hashtable<>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			htblColNameValue.put("id", 2343432 );
			htblColNameValue.put("name", "Ahmed Noor" );
			htblColNameValue.put("gpa", 0.95);
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 453455);
			htblColNameValue.put("name", "Ahmed Noor");
			htblColNameValue.put("gpa",  0.95);
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 5674567);
			htblColNameValue.put("name", "Dalia Noor");
			htblColNameValue.put("gpa",  1.25);
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 23498);
			htblColNameValue.put("name", "John Noor");
			htblColNameValue.put("gpa",  1.5);
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 78452 );
			htblColNameValue.put("name", "Zaky Noor");
			htblColNameValue.put("gpa",  0.88);
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			Table table = Serializer.deserializeTable(strTableName);
			assert table != null;
			table.printTable();


			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";

			arrSQLTerms[1] = new SQLTerm();
			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =   1.5;

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			System.out.println();
			while (resultSet.hasNext()){
				System.out.println(resultSet.next());
			}

			StringBuffer sqlBuffer = new StringBuffer("CREATE TABLE example_table (id INT PRIMARY KEY, name VARCHAR(50));");
			resultSet = dbApp.parseSQL(sqlBuffer);
			sqlBuffer = new StringBuffer("INSERT INTO example_table (id, name) VALUES (1, 'John'), (2, 'Sam');");
			resultSet = dbApp.parseSQL(sqlBuffer);
			sqlBuffer = new StringBuffer("SELECT * FROM example_table WHERE id >= 1 AND name = 'John';");
			resultSet = dbApp.parseSQL(sqlBuffer);
			while (resultSet.hasNext()){
				System.out.println(resultSet.next());
			}
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}