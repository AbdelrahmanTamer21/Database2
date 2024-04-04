package Main;
/** * @author Wael Abouelsaadat */

import Utilities.Serializer;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.*;
import Exception.DBAppException;


public class DBApp {

	public static int pageSize = readConfig("MaximumRowsCountinPage");
	public static int nodeOrder = readConfig("TreeNodeOrder");

	private HashSet<String> myTables;

	public DBApp( ){
		this.myTables = new HashSet<String>();
		init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		File pagesDir = new File("Pages");
		pagesDir.mkdir();
		File indicesDir = new File("Indices");
		indicesDir.mkdir();
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
			FileInputStream fileInputStream = new FileInputStream("DBApp.config");
			properties.load(fileInputStream);
			fileInputStream.close();

			return Integer.parseInt(properties.getProperty(property));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return 2;
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException {

		// first create file object for file placed at location specified by filepath
		// Create the metadata for the table
		File file = new File("metadata.csv");
		// if file does not exists, then create it
		try{
			if (!file.exists()) {
				file.createNewFile();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		if(checkTableMetadataExists(strTableName)){
			throw new DBAppException("Table already exists");
		}
		try{
			FileWriter outputFile = new FileWriter(file,true);

			//create CSVWriter with ',' as separator
			CSVWriter writer = new CSVWriter(outputFile, ',',
					CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER,
					CSVWriter.DEFAULT_LINE_END);

			// create a List which contains String array
			List<String[]> data = new ArrayList<>();
			Enumeration<String> e = htblColNameType.keys();
			LinkedHashMap<String, String> attr = new LinkedHashMap<>();
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
			Comparator<String[]> comparator = new Comparator<String[]>() {
				@Override
				public int compare(String[] arr1, String[] arr2) {
					boolean value1 = Boolean.parseBoolean(arr1[3]);
					boolean value2 = Boolean.parseBoolean(arr2[3]);

					// Put "true" values before "false" values
					return Boolean.compare(value2, value1); // Reverse order to put "true" before "false"
				}
			};
			data.sort(comparator);
			for (String[] row : data) {
				attr.put(row[1],row[2]);
			}
			writer.writeAll(data);
			Table table = new Table(strTableName,strClusteringKeyColumn,attr);
			// closing writer connection
			writer.close();

			myTables.add(strTableName);
			Serializer.serializeTable(table,strTableName);
		}catch (Exception e){
			e.printStackTrace();
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
	private void checkTableExits(String strTableName) throws DBAppException {
		if (!myTables.contains(strTableName)){
			throw new DBAppException("Table does not exist");
		}
	}

	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException {

		checkTableExits(strTableName);
		Table table = Serializer.deserializeTable(strTableName);
		table.createIndex(strColName,strIndexName);
		Serializer.serializeTable(table,strTableName);
		File file = new File("metadata.csv");
		try{
			FileReader inputFile = new FileReader(file);
			// Read existing file
			CSVReader reader = new CSVReader(inputFile, ',');
			List<String[]> csvBody = reader.readAll();
			// get CSV row column and replace with by using row and column
			for (int i = 0; i < csvBody.size(); i++) {
				String[] strArray = csvBody.get(i);
				if(strArray[0].equals(strTableName) && strArray[1].equals(strColName)){
					csvBody.get(i)[4] = strIndexName;
					csvBody.get(i)[5] = "B+Tree";
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
		checkTableExits(strTableName);
		Table table = Serializer.deserializeTable(strTableName);
		table.insertTuple(htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException {

		checkTableExits(strTableName);
		Table table = Serializer.deserializeTable(strTableName);
		table.updateTuple(strClusteringKeyValue,htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException {
		checkTableExits(strTableName);
		Table table = Serializer.deserializeTable(strTableName);
		table.deleteTuples(htblColNameValue);
		Serializer.serializeTable(table,strTableName);
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException {
										
		return null;
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

			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 5674567 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.25 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 23498 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 78452 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.88 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			Table table = Serializer.deserializeTable(strTableName);
			table.printTable();

			/*
			Main.SQLTerm[] arrSQLTerms;
			arrSQLTerms = new Main.SQLTerm[2];
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";

			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			 */
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}