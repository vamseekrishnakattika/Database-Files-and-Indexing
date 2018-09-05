package SqLite;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Operations{
	
	public static int pageSize = 512;
	public static String datePattern = "yyyy-MM-dd_HH:mm:ss";

	public static void main(String[] args){}
	
	public static int pages(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println(e);
		}

		return num_pages;
	}
	
	public static void createTable(String table, String[] col){
		try{	
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			
			int numOfPages = pages(file);
			int page=1;
			for(int p = 1; p <= numOfPages; p++){
				int rm = Index.getRightMost(file, p);
				if(rm == 0)
					page = p;
			}
			
			int[] keys = Index.getKeyArray(file, page);
			int l = keys[0];
			for(int i = 0; i < keys.length; i++)
				if(keys[i]>l)
					l = keys[i];
			file.close();
			
			String[] values = {Integer.toString(l+1), table};
			insertInto("davisbase_tables", values);

			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			
			numOfPages = pages(file);
			page=1;
			for(int p = 1; p <= numOfPages; p++){
				int rm = Index.getRightMost(file, p);
				if(rm == 0)
					page = p;
			}
			
			keys = Index.getKeyArray(file, page);
			l = keys[0];
			for(int i = 0; i < keys.length; i++)
				if(keys[i]>l)
					l = keys[i];
			file.close();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] token = col[i].split(" ");
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String nullable;
				if(token.length > 2)
					nullable = "NO";
				else
					 nullable = "YES";
				String[] value = {Integer.toString(l), table, col_name, dt, pos, nullable};
				insertInto("davisbase_columns", value);
			}
	
		}catch(Exception e){
			System.out.println(e);
		}
	}


	public static void insertInto(String table, String[] values){
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			insertInto(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void insertInto(RandomAccessFile file, String table, String[] values){
		String[] dtype = getDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL-value constraint violation");
				System.out.println();
				return;
			}

		int key = new Integer(values[0]);
		int page = searchKeyPage(file, key);
		if(page != 0)
			if(Index.hasKey(file, page, key)){
				System.out.println("Uniqueness constraint violation");
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[dtype.length-1];
		short plSize = (short) calPayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Index.checkLeafSpace(file, page, cellSize);


		if(offset != -1){
		Index.insertLeafCell(file, page, offset, plSize, key, stc, values);
		}else{
			Index.splitLeaf(file, page);
			insertInto(file, table, values);
		}
	}

	
	
	
	public static String[] retrieveValues(RandomAccessFile file, long loc){
		
		String[] values = null;
		try{
			
			SimpleDateFormat dateFormat = new SimpleDateFormat (datePattern);

			file.seek(loc+2);
			int key = file.readInt();
			int num_cols = file.readByte();
			
			byte[] stc = new byte[num_cols];
			file.read(stc);
			
			values = new String[num_cols+1];
			
			values[0] = Integer.toString(key);
			
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  file.readByte();
					            values[i] = "null";
								break;

					case 0x01:  file.readShort();
					            values[i] = "null";
								break;

					case 0x02:  file.readInt();
					            values[i] = "null";
								break;

					case 0x03:  file.readLong();
					            values[i] = "null";
								break;

					case 0x04:  values[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  values[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  values[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  values[i] = Long.toString(file.readLong());
								break;

					case 0x08:  values[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  values[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  Long temp = file.readLong();
								Date dateTime = new Date(temp);
								values[i] = dateFormat.format(dateTime);
								break;

					case 0x0B:  temp = file.readLong();
								Date date = new Date(temp);
								values[i] = dateFormat.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								file.read(bytes);
								values[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return values;
	}

	

	public static void update(String table, String[] cmp, String[] set){
		try{
			
			int key = new Integer(cmp[2]);
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int numPages = pages(file);
			int page = 0;
			for(int p = 1; p <= numPages; p++)
				if(Index.hasKey(file, p, key)&Index.getPageType(file, p)==0x0D){
					page = p;
				}
			
			if(page==0)
			{
				System.out.println("The given key value does not exist");
				return;
			}
			
			int[] keys = Index.getKeyArray(file, page);
			int x = 0;
			for(int i = 0; i < keys.length; i++)
				if(keys[i] == key)
					x = i;
			int offset = Index.getCellOffset(file, page, x);
			long loc = Index.getCellLoc(file, page, x);
			
			String[] cols = getColName(table);
			String[] values = retrieveValues(file, loc);

			String[] type = getDataType(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";

			for(int i = 0; i < cols.length; i++)
				if(cols[i].equals(set[0]))
					x = i;
			values[x] = set[2];

			String[] nullable = getNullable(table);
			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL-value constraint violation");
					return;
				}
			}

			byte[] stc = new byte[cols.length-1];
			int plsize = calPayloadSize(table, values, stc);
			Index.updateLeafCell(file, page, offset, plsize, key, stc, values);

			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void deleteFromTable(String table, String[] cmp){
		try{
		int key = new Integer(cmp[2]);

		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		int numPages = pages(file);
		int page = 0;
		for(int p = 1; p <= numPages; p++)
			if(Index.hasKey(file, p, key)&Index.getPageType(file, p)==0x0D){
				page = p;
				break;
			}
		
		if(page==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
		
		short[] cellsAddr = Index.getCellArray(file, page);
		int k = 0;
		for(int i = 0; i < cellsAddr.length; i++)
		{
			long loc = Index.getCellLoc(file, page, i);
			String[] vals = retrieveValues(file, loc);
			int x = new Integer(vals[0]);
			if(x!=key)
			{
				Index.setCellOffset(file, page, k, cellsAddr[i]);
				k++;
			}
		}
		Index.setCellNumber(file, page, (byte)k);
		
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
	}
	
	public static void dropTable(String table){
		try{
			
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page ++){
				file.seek((page-1)*pageSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellsAddr = Index.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellsAddr.length; i++)
					{
						long loc = Index.getCellLoc(file, page, i);
						String[] vals = retrieveValues(file, loc);
						String tb = vals[1];
						if(!tb.equals(table))
						{
							Index.setCellOffset(file, page, k, cellsAddr[i]);
							k++;
						}
					}
					Index.setCellNumber(file, page, (byte)k);
				}
				else
					continue;
			}

			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page ++){
				file.seek((page-1)*pageSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellsAddr = Index.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellsAddr.length; i++)
					{
						long loc = Index.getCellLoc(file, page, i);
						String[] vals = retrieveValues(file, loc);
						String tb = vals[1];
						if(!tb.equals(table))
						{
							Index.setCellOffset(file, page, k, cellsAddr[i]);
							k++;
						}
					}
					Index.setCellNumber(file, page, (byte)k);
				}
				else
					continue;
			}

			File anOldFile = new File("data", table+".tbl"); 
			anOldFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}
	
	public static int calPayloadSize(String table, String[] vals, byte[] stc){
		String[] dataType = getDataType(table);
		int size =dataType.length;
		for(int i = 1; i < dataType.length; i++){
			stc[i - 1]= getStc(vals[i], dataType[i]);
			size = size + feildLength(stc[i - 1]);
		}
		return size;
	}
	

	public static byte getStc(String value, String dataType){
		if(value.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	

    public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	public static void select(String table, String[] cols, String[] cmp){
		try{
			
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			String[] columnName = getColName(table);
			String[] type = getDataType(table);
			
			ReadWrite buffer = new ReadWrite();
			
			filter(file, cmp, columnName, type, buffer);
			buffer.display(cols);
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}


	
	public static int searchKeyPage(RandomAccessFile file, int key){
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Index.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Index.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}


	
	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			ReadWrite buffer = new ReadWrite();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] x : content.values()){
				array.add(x[3]);
			}
			int size=array.size();
			dataType = array.toArray(new String[size]);
			file.close();
			return dataType;
		}catch(Exception e){
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColName(String table){
		String[] cols = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			ReadWrite buffer = new ReadWrite();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			int size=array.size();
			cols = array.toArray(new String[size]);
			file.close();
			return cols;
		}catch(Exception e){
			System.out.println(e);
		}
		return cols;
	}

	public static String[] getNullable(String table){
		String[] nullable = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			ReadWrite buffer = new ReadWrite();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			int size=array.size();
			nullable = array.toArray(new String[size]);
			file.close();
			return nullable;
		}catch(Exception e){
			System.out.println(e);
		}
		return nullable;
	}



	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, ReadWrite buffer){
		try{
			
			int numOfPages = pages(file);
			
			for(int page = 1; page <= numOfPages; page++){
				
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				
					if(pageType == 0x0D){
						
					byte numOfCells = Index.getCellNumber(file, page);

					 for(int i=0; i < numOfCells; i++){
						long loc = Index.getCellLoc(file, page, i);
						String[] vals = retrieveValues(file, loc);
						int rowid=Integer.parseInt(vals[0]);
						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = "'"+vals[j]+"'";
						
						boolean check = cmpCheck(vals, rowid , cmp, columnName);

						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = vals[j].substring(1, vals[j].length()-1);

						if(check)
							buffer.add(rowid, vals);
					 }
				   }
				    else
						continue;
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}


	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, ReadWrite buffer){
		try{
			
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++){
				
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D)
				{
					byte numOfCells = Index.getCellNumber(file, page);

					for(int i=0; i < numOfCells; i++){
						
						long loc = Index.getCellLoc(file, page, i);	
						String[] vals = retrieveValues(file, loc);
						int rowid=Integer.parseInt(vals[0]);

						boolean check = cmpCheck(vals, rowid, cmp, columnName);
						
						if(check)
							buffer.add(rowid, vals);
					}
				}
				else
					continue;
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}


	
	public static boolean cmpCheck(String[] values, int rowid, String[] cmp, String[] columnName){

		boolean check = false;
		
		if(cmp.length == 0){
			check = true;
		}
		else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			
			if(colPos == 1){
				int val = Integer.parseInt(cmp[2]);
				String operator = cmp[1];
				switch(operator){
					case "=": if(rowid == val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= val) 
						        check = true;
					          else
					  	        check = false;	
					          break;
					case "<": if(rowid < val) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<=": if(rowid <= val) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "!=": if(rowid != val)  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(cmp[2].equals(values[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}
	
    
	
}