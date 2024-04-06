import BTree.BTree;
import Exception.DBAppException;
import Main.DBApp;
import Main.Page;
import Main.Table;
import Main.Tuple;
import Utilities.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class DBAppTest {
	private static DBApp engine;
	private static String newTableName;
	private static final String id = "id";
	private static final String name = "name";
	private static final String gpa = "gpa";
	private static final String TEST_NAME = "Abdp";
	private static final double TEST_GPA = 1.8;
	private static final String STRING_DATA_TYPE_NAME = "java.lang.String";
	private static final String INTEGER_DATA_TYPE_NAME = "java.lang.Integer";
	private static final String DOUBLE_DATA_TYPE_NAME = "java.lang.Double";

	private static void generateNewTableName() {
		int randomNumber1 = (int) (Math.random() * 100000) + 1;
		int randomNumber2 = (int) (Math.random() * 100000) + 1;
		StringBuilder s = new StringBuilder();
		newTableName = s.append("aa").append(randomNumber1).append(randomNumber2).toString();
		while (engine.getMyTables().contains(newTableName)) {
			randomNumber1 = (int) (Math.random() * 100000) + 1;
			randomNumber2 = (int) (Math.random() * 100000) + 1;
			s.setLength(0);
			newTableName = s.append(randomNumber1).append(randomNumber2).toString();
		}
	}

	private static void createTable() throws DBAppException {
		Hashtable<String, String> htblColNameType = createHashtable(INTEGER_DATA_TYPE_NAME,
				STRING_DATA_TYPE_NAME, DOUBLE_DATA_TYPE_NAME);

		engine.createTable(newTableName, id, htblColNameType);
	}

	@BeforeEach
	void setEnvironment() throws DBAppException{
		engine = new DBApp();
		engine.init();
		generateNewTableName();
		createTable();
	}

	@Test
	void testCreateTable_AlreadyExistingName_ShouldFailCreation() throws DBAppException {
		// Given
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.String");
		htblColNameType.put("courseName", "java.lang.String");
		// When
		Exception exception = assertThrows(DBAppException.class, () ->
				engine.createTable(newTableName, "id", htblColNameType)
		);
		// Then
		assertEquals("Table name already exists", exception.getMessage());
	}

	@Test
	void testCreateTable_InvalidPrimaryKeyColumn_ShouldFailCreation() throws DBAppException {
		// Given
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.String");
		htblColNameType.put("courseName", "java.lang.String");
		// When
		Exception exception = assertThrows(DBAppException.class, () ->
				engine.createTable("newTable", "price", htblColNameType)
		);
		// Then
		assertEquals("Clustering key is invalid", exception.getMessage());
	}

	@Test
	void testCreateTable_InvalidDataType_ShouldFailCreation() throws DBAppException {
		// Given
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.Byte");
		htblColNameType.put("courseName", "java.lang.String");
		// When
		Exception exception = assertThrows(DBAppException.class, () ->
				engine.createTable("newTable", "id", htblColNameType)
		);
		// Then
		assertEquals("Data type not supported", exception.getMessage());
	}


	@Test
	void testInsertIntoTable_OneTuple_ShouldInsertSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		Hashtable<String, Object> htblColNameValue = createRow(1, TEST_NAME, TEST_GPA);

		// When
		engine.insertIntoTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(1, Objects.requireNonNull(table).getPageNames().size());
		Page page = table.getPageAtPosition(0);
		assertEquals(1, page.getSize());
	}


	@Test
	void testInsertIntoTable_MissingColumn_ShouldInsertSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(gpa, TEST_GPA);
		htblColNameValue.put(id, 5);

		// When
		engine.insertIntoTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(1,table.getPageNames().size());
		Page page = table.getPageAtPosition(0);
		assertEquals(1, page.getSize());
	}


	@Test
	void testInsertIntoTable_ManyTuples_ShouldInsertSuccessfully() throws DBAppException {

		for (int i = 1; i < 300; i++) {
			// Given
			Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);

			// When
			engine.insertIntoTable(newTableName, htblColNameValue);
		}
		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(2, table.getPageNames().size());
		Page page = table.getPageAtPosition(1);
		assertEquals(99, page.getSize());
		page = table.getPageAtPosition(0);
		assertTrue(page.isFull());
	}

	@Test
	void testInsertIntoTable_InsertingLastRecordIntoFullPage_ShouldInsertSuccessfully() throws DBAppException {
		// Given
		for (int i = 2; i < 402; i += 2) {
			Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);
			engine.insertIntoTable(newTableName, htblColNameValue);
		}

		// When
		Hashtable<String, Object> htblColNameValue = createRow(399, TEST_NAME, TEST_GPA);
		engine.insertIntoTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(2, table.getPageNames().size());
		Page page = table.getPageAtPosition(0);
		assertTrue(page.isFull());
		assertEquals(399,page.getTuples().get(199).getPrimaryKeyValue());
	}

	@Test
	void testInsertIntoTable_InsertingRecordShiftingTwoPages_ShouldInsertSuccessfully() throws DBAppException {
		// Given
		for (int i = 2; i <= 802; i += 2) {
			Hashtable<String, Object> htblColNameValue = createRow(i, TEST_NAME, TEST_GPA);
			engine.insertIntoTable(newTableName, htblColNameValue);
		}

		// When
		Hashtable<String, Object> htblColNameValue = createRow(399, TEST_NAME, TEST_GPA);
		engine.insertIntoTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(3, table.getPageNames().size());
		Page page = table.getPageAtPosition(0);
		assertEquals(399,page.getTuples().get(199).getPrimaryKeyValue());
		page = table.getPageAtPosition(1);
		assertEquals(400,page.getTuples().get(0).getPrimaryKeyValue());
		page = table.getPageAtPosition(2);
		assertEquals(800,page.getTuples().get(0).getPrimaryKeyValue());
	}

	@Test
	void testInsertIntoTable_RepeatedPrimaryKey_ShouldFailInsert()
			throws DBAppException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = createRow(1, "moham", TEST_GPA);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.insertIntoTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "Primary key already exists";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage,outputMessage);
	}

	@Test
	void testInsertIntoTable_InvalidDataType_ShouldFailInsertion() throws DBAppException {
		// Given
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, "Foo");
		htblColNameValue.put(gpa, "boo");
		htblColNameValue.put(id, 55);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.insertIntoTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "Tuple's data type doesn't match the column's data type";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage,outputMessage);
	}

	@Test
	void testInsertIntoTable_MissingPrimaryKey_ShouldFailInsert() throws DBAppException {
		// Given
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(gpa, TEST_GPA);
		htblColNameValue.put(name, TEST_NAME);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.insertIntoTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "Primary key is not found";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage,outputMessage);
	}

	@Test
	void testInsertIntoTable_InvalidTableName_ShouldFailInsertion() {
		// Given
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, "Foo");
		htblColNameValue.put(gpa, TEST_GPA);
		htblColNameValue.put(id, 55);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.insertIntoTable("someRandomTableName", htblColNameValue);
		});

		// Then
		String expectedMessage = "Table does not exist";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage,outputMessage);
	}

	@Test
	void testInsertIntoTable_ExtraColumn_ShouldFailInsertion() {
		// Given
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, "Foo");
		htblColNameValue.put("salary", 10000);
		htblColNameValue.put(gpa, TEST_GPA);
		htblColNameValue.put(id, 3);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.insertIntoTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "Tuple contains columns that aren't in the table";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage,outputMessage);
	}

	@Test
	void testUpdateTable_ValidInput_ShouldUpdateSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		insertRow(1);
		String updatedName = "moham";
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, updatedName);

		// When
		engine.updateTable(newTableName, "1", htblColNameValue);

		// Then
		Page page = Serializer.deserializePage(newTableName, 1);
		Tuple updated = page.getTuples().get(0);
		assertEquals(updatedName,updated.getValues().get(name));
	}

	@Test
	void testUpdateTable_PrimaryKeyUpdate_ShouldFailUpdate()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(id, 2);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.updateTable(newTableName, "1", htblColNameValue);
		});

		// Then
		String expectedMessage = "The input row wants to change the primary key";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testUpdateTable_ExtraInput_ShouldFailUpdate() throws DBAppException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, "Foo");
		htblColNameValue.put(gpa, 1.8);
		htblColNameValue.put("University", "GUC");

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.updateTable(newTableName, "0", htblColNameValue);
		});

		// Then
		String expectedMessage = "The Tuple has more columns than the table's columns";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testUpdateTable_InvalidDataType_ShouldFailUpdate() throws DBAppException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(gpa, "Foo");

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.updateTable(newTableName, "1", htblColNameValue);
		});

		// Then
		String expectedMessage = "Tuple's data type doesn't match the column's data type";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testUpdateTable_InvalidTableName_ShouldFailUpdate() throws DBAppException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(gpa, 1.8);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.updateTable("randomName", "1", htblColNameValue);
		});

		// Then
		String expectedMessage = "Table does not exist";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testDeleteFromTable_OneTuple_ShouldDeleteSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException, InterruptedException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(id, 1);

		// When
		engine.deleteFromTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertTrue(table.isEmpty());
	}

	@Test
	void testDeleteFromTable_ManyTuplesDeleteOne_ShouldDeleteSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		for (int i = 0; i < 100; i++)
			insertRow(i);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, TEST_NAME);
		htblColNameValue.put(id, 0);

		// When
		engine.deleteFromTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(99, table.getSize());
	}

	@Test
	void testDeleteFromTable_ManyTuplesDeleteAll_ShouldDeleteSuccessfully()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		for (int i = 0; i < 100; i++)
			insertRow(i);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(name, TEST_NAME);

		// When
		engine.deleteFromTable(newTableName, htblColNameValue);

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertTrue(table.isEmpty());
	}

	@Test
	void testDeleteFromTable_InvalidColumnName_ShouldFailDelete()
			throws DBAppException, ClassNotFoundException, IOException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put("middle_name", "Mohamed");

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.deleteFromTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "The Tuple contains come columns that aren't in the table";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testDeleteFromTable_InvalidDataType_ShouldFailDelete() throws DBAppException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(id, 1);
		htblColNameValue.put("gpa", "Foo");

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.deleteFromTable(newTableName, htblColNameValue);
		});

		// Then
		String expectedMessage = "Tuple's data type doesn't match the column's data type";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testDeleteFromTable_InvalidTable_ShouldFailDelete()
			throws DBAppException, ClassNotFoundException, IOException, InterruptedException {
		// Given
		insertRow(1);
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(id, 1);

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.deleteFromTable("randomTableName", htblColNameValue);
		});

		// Then
		String expectedMessage = "Table does not exist";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testCreateIndex_ValidInput_ShouldCreateSuccessfully() throws DBAppException {
		// Given
		insertRow(1);

		// When
		engine.createIndex(newTableName, gpa, gpa+"Index");

		// Then
		Table table = Serializer.deserializeTable(newTableName);
		assertEquals(1,table.getBTrees().get(0).getRootKeyCount());
		assertEquals(1, table.getIndices().size());
	}

	@Test
	void testCreateIndex_RepeatedIndex_ShouldFailCreation() throws DBAppException {
		// Given
		engine.createIndex(newTableName, gpa, gpa+"Index");

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.createIndex(newTableName, name, gpa+"Index");
		});

		// Then
		String expectedMessage = "The index was already created on one of the columns";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);

	}

	@Test
	void testCreateIndex_InvalidTableName_ShouldFailCreation() throws DBAppException {

		// Given

		// When
		Exception exception = assertThrows(DBAppException.class, () -> {
			engine.createIndex("Foo", gpa, gpa);
		});

		// Then
		String expectedMessage = "Table does not exist";
		String outputMessage = exception.getMessage();
		assertEquals(expectedMessage, outputMessage);
	}

	@Test
	void testInsertionIntoIndex_ValidInput_ShouldInsertIntoIndex() throws DBAppException {
		// Given
		engine.createIndex(newTableName, gpa, gpa+"Index.ser");
		Table table = Serializer.deserializeTable(newTableName);
		int oldSize = table.getBTrees().get(0).getRootKeyCount();

		// When
		insertRow(1);

		// Then
		table = Serializer.deserializeTable(newTableName);
		int newSize = table.getBTrees().get(0).getRootKeyCount();
		assertEquals(oldSize+1, newSize);
	}

	//Comment if your btree doesn't contain these functions/add them to yours
	@Test
	void testUpdateTable_ValidInput_ShouldUpdateIndex() throws DBAppException {
		// Given
		engine.createIndex(newTableName, gpa, gpa+"Index.ser");
		insertRow(3);
		Table table = Serializer.deserializeTable(newTableName);
		boolean oldValue = ((BTree<Double, String>)table.getBTrees().get(0)).checkKeyExists(TEST_GPA);

		// When
		Hashtable<String, Object> updateTable = new Hashtable<>();
		updateTable.put("gpa", 0.7);
		engine.updateTable(newTableName, "3", updateTable);

		// Then
		table = Serializer.deserializeTable(newTableName);
		boolean oldValueCheck = ((BTree<Double, String>)table.getBTrees().get(0)).checkKeyExists(TEST_GPA);
		boolean newValueCheck = ((BTree<Double, String>)table.getBTrees().get(0)).checkKeyExists(0.7);
		assertTrue(oldValue);
		assertFalse(oldValueCheck);
		assertTrue(newValueCheck);
	}



	private static void insertRow(int id) throws DBAppException {

		Hashtable<String, Object> htblColNameValue = createRow(id, TEST_NAME, TEST_GPA);

		engine.insertIntoTable(newTableName, htblColNameValue);
	}

	private static Hashtable<String, String> createHashtable(String value1, String value2, String value3) {
		Hashtable<String, String> hashtable = new Hashtable<String, String>();
		hashtable.put(id, value1);
		hashtable.put(name, value2);
		hashtable.put(gpa, value3);
		return hashtable;
	}

	private static Hashtable<String, Object> createRow(int idInput, String nameInput, double gpaInput) {
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put(id, idInput);
		htblColNameValue.put(name, nameInput);
		htblColNameValue.put(gpa, gpaInput);
		return htblColNameValue;
	}

	/*
	@AfterEach
	void deleteCreatedTable() throws DBAppException {
		Table table = Serializer.deserializeTable(newTableName);
		FileDeleter.deleteFile(table, FileType.TABLE);
	}
	*/
}
