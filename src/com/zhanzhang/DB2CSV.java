package com.zhanzhang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * This class connects to a database and dumps all the tables and contents out
 * to CSV file
 */
public class DB2CSV {

	/** Dump the whole database to CSV files */
	public static String dumpDB(Properties props) {
		String driverClassName = props.getProperty("jdbc.driver.class");
		String driverURL = props.getProperty("jdbc.driver.url");
		String buffer = props.getProperty("buffer");
		int bufferSize = 1048576;// Default buffer size is 1024 * 1024 bytes (1MB)
		if (buffer != null && buffer.trim().length() > 0) {
			try {
				bufferSize = Integer.parseInt(buffer);
			} catch (Exception e) {

			}
		}
		DatabaseMetaData dbMetaData = null;
		Connection dbConn = null;
		try {
			Class.forName(driverClassName);
			dbConn = DriverManager.getConnection(driverURL, props);
			dbMetaData = dbConn.getMetaData();
		} catch (Exception e) {
			System.err.println("Unable to connect to database: " + e);
			return null;
		}

		try {
			StringBuffer result = new StringBuffer();
			String catalog = props.getProperty("catalog");
			String schema = props.getProperty("schemaPattern");
			String tables = props.getProperty("tableName");
			String exportLocation = props.getProperty("export.location");
			if (tables != null && tables.trim().length() == 0)
				tables = null;
			if (exportLocation == null || exportLocation.trim().length() == 0) {
				System.err.println("Failure: export.location is not set");
				return null;
			}
			ResultSet rs = dbMetaData.getTables(catalog, schema, tables, null);
			if (!rs.next()) {
				System.err.println("Unable to find any tables matching: catalog=" + catalog + " schema=" + schema
						+ " tables=" + tables);
				rs.close();
			} else {
				do {
					// Get the table column titles
					String tableName = rs.getString("TABLE_NAME");
					System.out.println("Working on table: " + tableName);
					ResultSet primaryKeys = dbMetaData.getPrimaryKeys(catalog, schema, tableName);
					List<String> primaryKeyNames = new ArrayList<String>();
					while (primaryKeys.next()) {
						primaryKeyNames.add(primaryKeys.getString("COLUMN_NAME"));
					}
					String tableType = rs.getString("TABLE_TYPE");
					if ("TABLE".equalsIgnoreCase(tableType)) {
						File file = new File(exportLocation + "/" + tableName + ".csv");
						if (file.exists()) {
							file.delete();
						}
						ResultSet tableMetaData = dbMetaData.getColumns(null, null, tableName, "%");
						boolean firstLine = true;
						while (tableMetaData.next()) {
							String columnName = tableMetaData.getString("COLUMN_NAME");
							if (firstLine) {
								firstLine = false;
								result.append(columnName);
							} else {
								result.append("," + columnName);
							}
						}
						if (result.length() > 0) {
							result.append("\n");
						}
						tableMetaData.close();
						// Populate the values
						dumpTable(dbConn, result, tableName, primaryKeyNames, bufferSize, exportLocation);
						writeToFile(tableName, result.toString(), exportLocation);
						System.out.println("Table: " + tableName + " has been exported into: " + exportLocation);
						result = new StringBuffer();
					}
				} while (rs.next());
				rs.close();
			}
			dbConn.close();
			return result.toString();
		} catch (SQLException | IOException e) {
			e.printStackTrace(); // To change body of catch statement use Options | File Templates.
		}
		return null;
	}

	/**
	 * dump this particular table data
	 * 
	 * @throws IOException
	 */
	private static void dumpTable(Connection dbConn, StringBuffer result, String tableName, List<String> primiaryKeys,
			int bufferSize, String exportLocation) throws IOException {
		SimpleDateFormat timeStampFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		try {
			// First we output the create table stuff
			PreparedStatement stmt = dbConn.prepareStatement("SELECT * FROM " + tableName);
			ResultSet rs = stmt.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			// Now we can output the actual data
			while (rs.next()) {
				String fileName = null;
				for (int i = 0; i < columnCount; i++) {
					if (i > 0) {
						result.append(",");
					}
					Object value = rs.getObject(i + 1);
					if (value == null) {
						value = "";
					}
					int type = metaData.getColumnType(i + 1);
					String columnName = metaData.getColumnName(i+1);
					if (primiaryKeys!=null && primiaryKeys.contains(columnName)) {
						if (fileName==null)
							fileName = value.toString();
						else {
							fileName = fileName + "+" + value.toString(); 
						}
					}
					if (type == Types.LONGVARCHAR || type == Types.LONGNVARCHAR) { // Process Blob text (Not LONGVARBINARY)
						InputStream inputStream = rs.getBinaryStream(i + 1);
						if (inputStream!=null) {
							InputStreamReader isr = new InputStreamReader(inputStream);
							BufferedReader br = new BufferedReader(isr);
							String line;
							result.append("\""); // Put double quote before and after string to escape comma
							while ((line = br.readLine()) != null) {
								if (line.indexOf("\"") != -1) {
									line = line.replaceAll("\"", "\"\"");
								}
								result.append(line);
								result.append("\n");
							}
							result.append("\"");
							inputStream.close();
						} else {
							result.append("\"\"");
						}
					} else if (type == Types.LONGVARBINARY) { // Process blob binary
						String folderName = exportLocation + "\\" + tableName + "\\";
						File directory = new File(folderName);
						if (!directory.exists()) {
							directory.mkdirs();
						}
						if (fileName == null) {
							//Generate random file name
							fileName = UUID.randomUUID().toString();
						}
						File file = new File(directory.getAbsolutePath() + "\\" + fileName);
						if (file.exists()) {
							file.delete();
						}
						file.createNewFile();
						FileOutputStream fos = new FileOutputStream(file);
						Blob b = rs.getBlob(i + 1);
						if (b!=null) {
							InputStream inputStream = b.getBinaryStream();
							int readNum = 0;
							byte[] buffer = new byte[1024];
							while ((readNum = inputStream.read(buffer)) != -1) {
								fos.write(buffer, 0, readNum);
							}							
						}
						fos.close();
						result.append("\"" + file.getAbsolutePath() + "\"");
					} else if (type == Types.DATE) {
						try {
							value = dateFormat.format(rs.getDate(i + 1));
						} catch (Exception e) {
							// DOn't do anything. Keep the old value
						}
						result.append("\"" + value + "\"");
					} else if (type == Types.TIMESTAMP) {
						try {
							value = timeStampFormat.format(rs.getTimestamp(i + 1));
						} catch (Exception e) {
							// DOn't do anything. Keep the old value
						}
						result.append("\"" + value + "\"");
					} else { 
						result.append("\"" + value.toString()+ "\"");
					}

				}
				result.append("\n");
				if (result.length() >= bufferSize) {
					writeToFile(tableName, result.toString(), exportLocation);
					System.out.println(Calendar.getInstance().getTime() + " Exporting table: " + tableName + "'s " + result.length() / 1024 + "KB data into " + exportLocation + " to clean up memory buffer");
					result.delete(0, result.length());
				}
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void writeToFile(String tableName, String content, String exportLocation) throws IOException {
		File directory = new File(exportLocation);
		if (!directory.exists()) {
			directory.mkdirs();
		}
		File file = new File(directory.getAbsolutePath() + "\\" + tableName + ".csv");
		if (!file.exists())
			file.createNewFile();
		FileWriter fr = new FileWriter(file, true); // Append the content to the end
		fr.write(content);
		fr.close();
	}

	public static void main(String[] args) {
		Calendar startCalendar = Calendar.getInstance();
		long startTime = startCalendar.getTimeInMillis();
		Properties props = new Properties();
		InputStream inputStream = DB2CSV.class.getResourceAsStream("configuration.properties");
		try {
			props.load(inputStream);
			System.out.println("Exporting database...");
			dumpDB(props);
			Calendar endCalendar = Calendar.getInstance();
			long finishTime = endCalendar.getTimeInMillis();
			float elapsedTime = (finishTime - startTime) / 1000 / 60;
			System.out.println("Total elapsed time: " + elapsedTime + " minutes");
			System.out.println("Done");
		} catch (IOException e) {
			System.err.println("Unable to open property file: configuration.properties exception: " + e);
		}

	}
}