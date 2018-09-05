package SqLite;

import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class  SqLite {

	
	static String prompt = "SqLite> ";
	static String version = "v2.0.1.7";
	static boolean isExit = false;
	static String copyright = "�2017 Vamseekrishna Kattika";
		
	public static int pageSize = 512;
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	public static void main(String[] args) {    	
		
		init();		
		splashScreen();		
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		
		System.out.println("Exiting...");


	}

    
	
    
    public static String getCopyright() {
		return copyright;
	}
	
    public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBase");
		System.out.println("DavisBase Version " + version);
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSHOW TABLES;                                               Display all the tables in the database.");
		System.out.println("\tCREATE TABLE table_name (<column_name datatype>);          Create a new table in the database.");
		System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);          Insert a new record into the table.");
		System.out.println("\tDELETE FROM TABLE table_name WHERE row_id = key_value;     Delete a record from the table whose rowid is <key_value>.");
		System.out.println("\tUPDATE table_name SET column_name = value WHERE condition; Modifies the records in the table.");
		System.out.println("\tSELECT * FROM table_name;                                  Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE column_name operator value; Display records in the table where the given condition is satisfied.");
		System.out.println("\tDROP TABLE table_name;                                     Remove table data and its schema.");
		System.out.println("\tVERSION;                                                   Show the program version.");
		System.out.println("\tHELP;                                                      Show this help information.");
		System.out.println("\tEXIT;                                                      Exit the program.");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	
	public static void init(){
		try {
			File dataDir = new File("data");
			if(dataDir.mkdir()){
				System.out.println("The data base doesn't exit, initializing data base...");
				initialize();
			}
			else {
				
				String[] oldTableFiles = dataDir.list();
				boolean checkTab = false;
				boolean checkCol = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals("davisbase_tables.tbl"))
						checkTab = true;
					if(oldTableFiles[i].equals("davisbase_columns.tbl"))
						checkCol = true;
				}
				
				if(!checkTab){
					System.out.println("The davisbase_tables does not exit, initializing data base...");
					System.out.println();
					initialize();
				}
				
				if(!checkCol){
					System.out.println("The davisbase_columns table does not exit, initializing data base...");
					System.out.println();
					initialize();
				}
				
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

	}

	public static boolean tableExists(String tablename){
		tablename = tablename+".tbl";
		
		try {
			File dataDir = new File("data");
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				if(oldTableFiles[i].equals(tablename))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		return false;
	}
	
	

	public static String[] parserEquation(String equ){
		String comparator[] = new String[3];
		String temp[] = new String[2];
		if(equ.contains("=")) {
			temp = equ.split("=");
			comparator[0] = temp[0].trim();
			comparator[1] = "=";
			comparator[2] = temp[1].trim();
		}

		if(equ.contains("<")) {
			temp = equ.split("<");
			comparator[0] = temp[0].trim();
			comparator[1] = "<";
			comparator[2] = temp[1].trim();
		}
		
		if(equ.contains(">")) {
			temp = equ.split(">");
			comparator[0] = temp[0].trim();
			comparator[1] = ">";
			comparator[2] = temp[1].trim();
		}
		
		if(equ.contains("<=")) {
			temp = equ.split("<=");
			comparator[0] = temp[0].trim();
			comparator[1] = "<=";
			comparator[2] = temp[1].trim();
		}

		if(equ.contains(">=")) {
			temp = equ.split(">=");
			comparator[0] = temp[0].trim();
			comparator[1] = ">=";
			comparator[2] = temp[1].trim();
		}

		return comparator;
	}
		
	public static void parseUserCommand (String userCommand) {
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {

		    case "show":
			    showTables();
			    break;
			
		    case "create":
				parseCreateString(userCommand);
			    break;

			case "insert":
				parseInsertString(userCommand);
				break;
				
			case "delete":
				parseDeleteString(userCommand);
				break;	

			case "update":
				parseUpdateString(userCommand);
				break;
				
			case "select":
				parseQueryString(userCommand);
				break;

			case "drop":
				dropTable(userCommand);
				break;	

			case "help":
				help();
				break;

			case "version":
				System.out.println("DavisBase Version " + version);
				System.out.println(copyright);
				break;

			case "exit":
				isExit=true;
				break;
				
			case "quit":
				isExit=true;
				break;
	
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				System.out.println();
				break;
		}
	} 

	public static  void showTables() {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"show tables\"");
		
		String table = "davisbase_tables";
		String[] cols = {"table_name"};
		String[] cmptr = new String[0];
		Operations.select(table, cols, cmptr);
	}
	
    public static void parseCreateString(String createString) {
		
		System.out.println("STUB: Calling your method to process the command");
		System.out.println("Parsing the string:\"" + createString + "\"");
		
		String[] tokens=createString.split(" ");
		String tableName = tokens[2];
		String[] temp = createString.split(tableName);
		String cols = temp[1].trim();
		String[] create_cols = cols.substring(1, cols.length()-1).split(",");
		
		for(int i = 0; i < create_cols.length; i++)
			create_cols[i] = create_cols[i].trim();
		
		if(tableExists(tableName)){
			System.out.println("Table "+tableName+" already exists.");
		}
		else
			{
			Operations.createTable(tableName, create_cols);		
			}

	}
    
    public static void parseQueryString(String queryString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		
		String[] cmp;
		String[] column;
		String[] temp = queryString.split("where");
		if(temp.length > 1){
			String tmp = temp[1].trim();
			cmp = parserEquation(tmp);
		}
		else{
			cmp = new String[0];
		}
		String[] select = temp[0].split("from");
		String tableName = select[1].trim();
		String cols = select[0].replace("select", "").trim();
		if(cols.contains("*")){
			column = new String[1];
			column[0] = "*";
		}
		else{
			column = cols.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}
		else
		{
		    Operations.select(tableName, column, cmp);
		}
	}
    
    public static void parseInsertString(String insertString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + insertString + "\"");
		
		String[] tokens=insertString.split(" ");
		String table = tokens[2];
		String[] temp = insertString.split("values");
		String temporary=temp[1].trim();
		String[] insert_vals = temporary.substring(1, temporary.length()-1).split(",");
		for(int i = 0; i < insert_vals.length; i++)
			insert_vals[i] = insert_vals[i].trim();
		if(!tableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Operations.insertInto(table, insert_vals);
		}

	}
    
    public static void parseUpdateString(String updateString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + updateString + "\"");
		
		String[] tokens=updateString.split(" ");
		String table = tokens[1];
		String[] temp1 = updateString.split("set");
		String[] temp2 = temp1[1].split("where");
		String cmpTemp = temp2[1];
		String setTemp = temp2[0];
		String[] cmp = parserEquation(cmpTemp);
		String[] set = parserEquation(setTemp);
		if(!tableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Operations.update(table, cmp, set);
		}
		
	}
    
    public static void parseDeleteString(String deleteString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + deleteString + "\"");
		
		String[] tokens=deleteString.split(" ");
		String table = tokens[3];
		String[] temp = deleteString.split("where");
		String cmpTemp = temp[1];
		String[] cmp = parserEquation(cmpTemp);
		if(!tableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Operations.deleteFromTable(table, cmp);
		}
		
		
	}
        
	
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		
		String[] tokens=dropTableString.split(" ");
		String tableName = tokens[2];
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}
		else
		{
			Operations.dropTable(tableName);
		}		

	}
	public static void initialize() {

		
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile tablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			tablesCatalog.setLength(pageSize);
			tablesCatalog.seek(0);
			tablesCatalog.write(0x0D);
			tablesCatalog.writeByte(0x02);
			
			int size1=24;
			int size2=25;
			
			int offsetT=pageSize-size1;
			int offsetC=offsetT-size2;
			
			tablesCatalog.writeShort(offsetC);
			tablesCatalog.writeInt(0);
			tablesCatalog.writeInt(0);
			tablesCatalog.writeShort(offsetT);
			tablesCatalog.writeShort(offsetC);
			
			tablesCatalog.seek(offsetT);
			tablesCatalog.writeShort(20);
			tablesCatalog.writeInt(1); 
			tablesCatalog.writeByte(1);
			tablesCatalog.writeByte(28);
			tablesCatalog.writeBytes("davisbase_tables");
			
			tablesCatalog.seek(offsetC);
			tablesCatalog.writeShort(21);
			tablesCatalog.writeInt(2); 
			tablesCatalog.writeByte(1);
			tablesCatalog.writeByte(29);
			tablesCatalog.writeBytes("davisbase_columns");
			
			tablesCatalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
		
		try {
			RandomAccessFile columnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			columnsCatalog.setLength(pageSize);
			columnsCatalog.seek(0);       
			columnsCatalog.writeByte(0x0D); 
			columnsCatalog.writeByte(0x08); 
			
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			
			columnsCatalog.writeShort(offset[7]); 
			columnsCatalog.writeInt(0); 
			columnsCatalog.writeInt(0); 
			
			for(int i=0;i<8;i++)
				columnsCatalog.writeShort(offset[i]);

			
			columnsCatalog.seek(offset[0]);
			columnsCatalog.writeShort(33);
			columnsCatalog.writeInt(1); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(28);
			columnsCatalog.writeByte(17);
			columnsCatalog.writeByte(15);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_tables"); 
			columnsCatalog.writeBytes("rowid"); 
			columnsCatalog.writeBytes("INT"); 
			columnsCatalog.writeByte(1); 
			columnsCatalog.writeBytes("NO"); 	
			
			columnsCatalog.seek(offset[1]);
			columnsCatalog.writeShort(39); 
			columnsCatalog.writeInt(2); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(28);
			columnsCatalog.writeByte(22);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_tables"); 
			columnsCatalog.writeBytes("table_name"); 
			columnsCatalog.writeBytes("TEXT"); 
			columnsCatalog.writeByte(2);
			columnsCatalog.writeBytes("NO"); 
			
			columnsCatalog.seek(offset[2]);
			columnsCatalog.writeShort(34); 
			columnsCatalog.writeInt(3); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(17);
			columnsCatalog.writeByte(15);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("rowid");
			columnsCatalog.writeBytes("INT");
			columnsCatalog.writeByte(1);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[3]);
			columnsCatalog.writeShort(40);
			columnsCatalog.writeInt(4); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(22);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("table_name");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(2);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[4]);
			columnsCatalog.writeShort(41);
			columnsCatalog.writeInt(5); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(23);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("column_name");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(3);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[5]);
			columnsCatalog.writeShort(39);
			columnsCatalog.writeInt(6); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(21);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("data_type");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(4);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[6]);
			columnsCatalog.writeShort(49); 
			columnsCatalog.writeInt(7); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(28);
			columnsCatalog.writeByte(19);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("ordinal_position");
			columnsCatalog.writeBytes("TINYINT");
			columnsCatalog.writeByte(5);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[7]);
			columnsCatalog.writeShort(41); 
			columnsCatalog.writeInt(8); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(23);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("is_nullable");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(6);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
}
		

}