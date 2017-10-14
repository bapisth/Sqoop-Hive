package com.hemendra.sqoop.uitl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

public class SqoopUtility {

	private static SqoopOptions SqoopOptions = null;
	private static final String connectionString = "jdbc:mysql://192.168.1.224:3306/test_ananya";
	private static final String databaseUsername = "karadonga";
	private static final String databasePassword = "Karadonga1";

	static {
		SqoopOptions = new SqoopOptions();
		SqoopOptions.setHadoopMapRedHome(System.getenv("HADOOP_MAPRED_HOME"));
	}

	public static String getConnectionstring() {
		return connectionString;
	}

	public static String getDatabaseusername() {
		return databaseUsername;
	}

	public static String getDatabasepassword() {
		return databasePassword;
	}

	private static void setUp() {
		SqoopOptions.setConnectString(getConnectionstring());
		SqoopOptions.setUsername(getDatabaseusername());
		SqoopOptions.setPassword(getDatabasepassword());
	}

	private static int runIt() {
		int res;
		res = new ImportTool().run(SqoopOptions);
		if (res != 0) {
			throw new RuntimeException("Sqoop API Failed - return code : " + Integer.toString(res));
		}
		return res;
	}

	private static void TransferringEntireTable(String table) {
		SqoopOptions.setTableName(table);
	}

	private static void TansferringEntireTableSpecificDir(String table, String directory) {
		TransferringEntireTable(table);
		SqoopOptions.setWarehouseDir(directory);
	}

	private static void TansferringEntireTableSpecificDirHiveMerge(String table, String directory) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	private static void TansferringEntireTableSpecificDirHivePartitionMerge(String table, String directory,
			String partitionKey, String partitionValue) {
		TansferringEntireTableSpecificDirHiveMerge(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}

	private static void TansferringEntireTableWhereClause(String table, String whereClause) {
		// To do
	}

	private static void CompressingImportedData(String table, String directory, String compress) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setCompressionCodec(compress);
	}

	private static void incrementalImport(String table, String directory, IncrementalMode mode, String checkColumn,
			String lastVale) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setIncrementalMode(mode);
		SqoopOptions.setAppendMode(true);
		SqoopOptions.setIncrementalTestColumn(checkColumn);
		SqoopOptions.setIncrementalLastValue(lastVale);
	}

	private static void TransferringEntireTableSpecificDirHive(String table, String directory) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	private static void TransferringEntireTableSpecificDirHivePartition(String table, String directory,
			String partitionKey, String partitionValue) {
		TransferringEntireTableSpecificDirHive(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}

	/**
	 * Not working for now
	 * 
	 * @return
	 */
	private static int performHdfsImport() {
		SqoopTool tool = new ImportTool();
		String[] args = { "--connection-manager", "com.mysql.jdbc.Driver", "--connect",
				"mysql://192.168.1.224:3306/test_ananya", "--username", "karadonga", "--password", "Karadonga1",
				"--table", "employee", "--target-dir", "/myJavaSqoopTest" };

		List<String> sqoopImportArgs = new ArrayList<String>();
		/*sqoopImportArgs.add("--connection-manager");
		sqoopImportArgs.add("com.mysql.jdbc.Driver");*/

		sqoopImportArgs.add("--connect");
		sqoopImportArgs.add("jdbc:mysql://192.168.1.224:3306/test_ananya");

		sqoopImportArgs.add("--username");
		sqoopImportArgs.add("karadonga");

		sqoopImportArgs.add("--password");
		sqoopImportArgs.add("Karadonga1");
		
		
		sqoopImportArgs.add("--hadoop-home");
		sqoopImportArgs.add(System.getenv("HADOOP_HOME"));
		
		sqoopImportArgs.add("--hadoop-mapred-home");
		sqoopImportArgs.add(System.getenv("HADOOP_MAPRED_HOME"));

		sqoopImportArgs.add("--table");
		sqoopImportArgs.add("employee");

		sqoopImportArgs.add("--target-dir");
		sqoopImportArgs.add("mysqoopPlace");
		
		
		
		// --hive-import --create-hive-table --hive-table sqoopdb.mysqlWorld
		
		sqoopImportArgs.add("--hive-import");
		sqoopImportArgs.add("--create-hive-table");
		sqoopImportArgs.add("--hive-table");
		sqoopImportArgs.add("sqoopdb.hemufromjava");

		sqoopImportArgs.add("-m");
		sqoopImportArgs.add("1");

		String[] importArgsArr = sqoopImportArgs.toArray(new String[sqoopImportArgs.size()]);

		SqoopOptions options = new SqoopOptions();

		try {
			options = tool.parseArguments(importArgsArr, null, options, false);
			tool.validateOptions(options);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			// throw new Exception(e.message);
		}
		return tool.run(options);
	}

	public static void main(String[] args) {

		//performHdfsImport();

		/*
		 * SqoopExamples examples = new SqoopExamples();
		 * 
		 * int performHdfsImport = examples.performHdfsImport();
		 * 
		 * System.out.println("Hdfs result :" + performHdfsImport);
		 */

		/**
		 * Transfering data directly to Hive table
		 */
		
		 setUp(); String tableName = "employee";
		 TransferringEntireTableSpecificDirHive(tableName, "asadtsdfal/input3");
		 runIt();
		
		

		/**
		 * Load Entire table
		 */

		/*System.out.println(SqoopOptions.getHadoopMapRedHome());

		System.out.println(System.getenv("HADOOP_COMMON_LIB_NATIVE_DIR"));
		System.out.println(System.getenv("HADOOP_OPTS"));

		setUp();
		String tableName = "employee";
		TransferringEntireTable(tableName);
		runIt();*/

	}

}
