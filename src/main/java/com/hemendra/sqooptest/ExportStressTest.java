package com.hemendra.sqooptest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

public class ExportStressTest extends Configured implements Tool {

	// Export 10 GB of data. Each record is ~100 bytes.
	public static final int NUM_FILES = 10;
	public static final int RECORDS_PER_FILE = 10 * 1024 * 1024;

	public static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	public ExportStressTest() {
	}

	public void createFile(int fileId) throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		String pathString = "ExportStressTest-12";
		Path dirPath = new Path(pathString);
		fs.mkdirs(dirPath);
		Path filePath = new Path(dirPath, "input-" + fileId);

		OutputStream os = fs.create(filePath);
		Writer w = new BufferedWriter(new OutputStreamWriter(os));
		for (int i = 0; i < RECORDS_PER_FILE; i++) {
			long v = (long) i + ((long) RECORDS_PER_FILE * (long) fileId);
			w.write("" + v + "," + ALPHABET + ALPHABET + ALPHABET + ALPHABET + "\n");

		}
		w.close();
		os.close();
	}

	/** Create a set of data files to export. */
	public void createData() throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path("ExportStressTest-12");
		if (fs.exists(dirPath)) {
			System.out.println("Export directory appears to already exist. Skipping data-gen.");
			return;
		}

		for (int i = 0; i < NUM_FILES; i++) {
			createFile(i);
		}
	}

	/** Create a table to hold our results. Drop any existing definition. */
	public void createTable(String connectStr, String username, String password) throws Exception {
		Class.forName("com.mysql.jdbc.Driver"); // Load mysql driver.

		Connection conn = DriverManager.getConnection(connectStr, username, password);
		conn.setAutoCommit(false);
		PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS ExportStressTestTable",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.executeUpdate();
		stmt.close();

		stmt = conn.prepareStatement(
				"CREATE TABLE ExportStressTestTable(id INT NOT NULL PRIMARY KEY, " + "msg VARCHAR(110)) Engine=InnoDB",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.executeUpdate();
		stmt.close();
		conn.commit();
		conn.close();
	}

	/**
	 * Actually run the export of the generated data to the user-created table.
	 */
	public void runExport(String connectStr, String username, String password) throws Exception {
		SqoopOptions options = new SqoopOptions(getConf());
		options.setConnectString(connectStr);
		options.setTableName("ExportStressTestTable");
		options.setUsername(username);
		options.setPassword(password);
		options.setExportDir("ExportStressTest");
		options.setNumMappers(1);
		options.setLinesTerminatedBy('\n');
		options.setFieldsTerminatedBy(',');
		options.setExplicitOutputDelims(true);

		SqoopTool exportTool = new ExportTool();
		Sqoop sqoop = new Sqoop(exportTool, getConf(), options);
		int ret = Sqoop.runSqoop(sqoop, new String[0]);
		if (0 != ret) {
			throw new Exception("Error doing export; ret=" + ret);
		}
	}

	/**
	 * Actually run the import of the data from the user-created table.
	 */
	public void runImport(String connectStr, String username, String password) throws Exception {
		/**
		 * sqoop import --connect jdbc:mysql://192.168.1.224:3306/test_ananya --username karadonga -P --table employee --target-dir /hemu -m 1
		 */
		 
		SqoopOptions options = new SqoopOptions(getConf()); //sqoop
		options.setConnectString(connectStr); //--connect <connection-url>
		options.setUsername(username); //--username
		options.setPassword(password);//--password
		options.setTableName("employee"); //--table <tableName>
		
		//ptions.setTargetDir("/hemuFromJava");
		//options.setExportDir("/hemu");
		options.setNumMappers(1);
		options.setWarehouseDir("/kadali");
		//options.setFieldsTerminatedBy(',');
		
		SqoopTool importTool = new ImportTool();
		Sqoop sqoop = new Sqoop(importTool, getConf(), options);
		int ret = Sqoop.runSqoop(sqoop, new String[0]);
		if (0 != ret) {
			throw new Exception("Error doing export; ret=" + ret);
		}

		/*SqoopTool importTool = new ImportTool();
		
		int importResult = importTool.run(options);
		if (0 != importResult) {
			throw new Exception("Error doing import; importResult=" + importResult);
		}*/
		
		//Test2
		
		/*List<String> sqoopImportArgs = new ArrayList<String>();
		
		sqoopImportArgs.add("import");
		sqoopImportArgs.add("--connect");
		sqoopImportArgs.add("jdbc:mysql://192.168.1.224:3306/test_ananya");
		sqoopImportArgs.add("--username");
		sqoopImportArgs.add("karadonga");
		sqoopImportArgs.add("--password");
		sqoopImportArgs.add("Karadonga1");
		sqoopImportArgs.add("--table");
		sqoopImportArgs.add("--employee");
		sqoopImportArgs.add("--target-dir");
		sqoopImportArgs.add("/hemuFromJava1");
		sqoopImportArgs.add("-m");
		sqoopImportArgs.add("1");
		
		String[] sqoopImportArgsArr = sqoopImportArgs.toArray(new String[sqoopImportArgs.size()]);
		
		
		
		
		int runTool = Sqoop.runTool(sqoopImportArgsArr, getConf());
		if (0 != runTool) {
			throw new Exception("Error doing import; ret=" + runTool);
		}*/
		
		
		
		/*Sqoop sqoop = new Sqoop(importTool, getConf(), options);

		int importData = sqoop.runSqoop(sqoop, new String[0]);
		if (0 != importData) {
			throw new Exception("Error doing import; ret=" + importData);
		}*/

		/*
		 * SqoopTool exportTool = new ExportTool(); Sqoop sqoop = new
		 * Sqoop(exportTool, getConf(), options); int ret =
		 * Sqoop.runSqoop(sqoop, new String[0]); if (0 != ret) { throw new
		 * Exception("Error doing export; ret=" + ret); }
		 */
	}

	@Override
	public int run(String[] args) {
		String connectStr = "";// args[0];
		String username = "";// args[1];
		String password = "";

		connectStr = "jdbc:mysql://192.168.1.224:3306/test_ananya";
		username = "karadonga";
		password = "Karadonga1";

		try {
			//createData();
			//createTable(connectStr, username, password);
			//runExport(connectStr, username, password);
			
			runImport(connectStr, username, password);
		} catch (Exception e) {
			System.err.println("Error: " + StringUtils.stringifyException(e));
			return 1;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ExportStressTest test = new ExportStressTest();
		int ret = ToolRunner.run(test, args);
		System.exit(ret);
	}

}
