package com.hemendra.sqoop.uitl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

public class SqoopUtility extends Configured {

	private static SqoopOptions SqoopOptions = null;
	private static final String connectionString = "";
	private static final String connectionStringMYSQL = "jdbc:mysql://192.168.1.224:3306/test_ananya";
	private static final String connectionStringPOSTGRE_SQL = "jdbc:postgresql://192.168.1.224:5432/ims_prod";
	private static final String databaseUsername = "karadonga";
	private static final String databasePassword = "Karadonga1";
	
	private static final String databaseUsernamePostgres = "postgres";
	private static final String databasePasswordPostgres = "karadonga@1";

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
	

	public static String getDatabaseusernamepostgres() {
		return databaseUsernamePostgres;
	}

	public static String getDatabasepasswordpostgres() {
		return databasePasswordPostgres;
	}

	public static String getConnectionstringmysql() {
		return connectionStringMYSQL;
	}

	public static String getConnectionstringpostgreSql() {
		return connectionStringPOSTGRE_SQL;
	}

	private static void setUp() {
		SqoopOptions.setConnectString(getConnectionstringpostgreSql());
		//SqoopOptions.setConnectString(getConnectionstring());
		//SqoopOptions.setConnectString(getConnectionstring());
		
		//SqoopOptions.setUsername(getDatabaseusername());
		//SqoopOptions.setPassword(getDatabasepassword());
		
		SqoopOptions.setUsername(getDatabaseusernamepostgres());
		SqoopOptions.setPassword(getDatabasepasswordpostgres());
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
	@SuppressWarnings("deprecation")
	private static int performHdfsImportHem() {
		SqoopTool tool = new ImportTool();
		String[] args = { "--connection-manager", "com.mysql.jdbc.Driver", "--connect",
				"mysql://192.168.1.224:3306/test_ananya", "--driver", "com.mysql.jdbc.Driver", "--username",
				"karadonga", "--password", "Karadonga1", "--table", "employee", "--target-dir", "myJavaSqoopTest" };

		/*
		 * String hadoopHome = System.getenv("HADOOP_HOME");
		 * 
		 * List<String> sqoopImportArgs = new ArrayList<String>();
		 * sqoopImportArgs.add("--connection-manager");
		 * sqoopImportArgs.add("com.mysql.jdbc.Driver");
		 * 
		 * sqoopImportArgs.add("--connect");
		 * sqoopImportArgs.add("jdbc:mysql://192.168.1.224:3306/test_ananya");
		 * 
		 * sqoopImportArgs.add("--username"); sqoopImportArgs.add("karadonga");
		 * 
		 * sqoopImportArgs.add("--password"); sqoopImportArgs.add("Karadonga1");
		 * 
		 * 
		 * sqoopImportArgs.add("--hadoop-home");
		 * 
		 * sqoopImportArgs.add(hadoopHome);
		 * 
		 * sqoopImportArgs.add("--hadoop-mapred-home");
		 * sqoopImportArgs.add(hadoopHome + "/share/hadoop/mapreduce");
		 * 
		 * sqoopImportArgs.add("--table"); sqoopImportArgs.add("employee");
		 * 
		 * sqoopImportArgs.add("--target-dir");
		 * sqoopImportArgs.add("mysqoopPlace2");
		 * 
		 * 
		 * 
		 * // --hive-import --create-hive-table --hive-table sqoopdb.mysqlWorld
		 * 
		 * sqoopImportArgs.add("--hive-import");
		 * sqoopImportArgs.add("--create-hive-table");
		 * sqoopImportArgs.add("--hive-table");
		 * sqoopImportArgs.add("sqoopdb.hemufromjava");
		 * 
		 * sqoopImportArgs.add("-m"); sqoopImportArgs.add("1");
		 * 
		 * String[] importArgsArr = sqoopImportArgs.toArray(new
		 * String[sqoopImportArgs.size()]);
		 */

		SqoopOptions options = new SqoopOptions();
		// Configuration conf = options.getConf();

		try {
			options = tool.parseArguments(args, null, options, false);
			tool.validateOptions(options);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			// throw new Exception(e.message);
		}

		// String HadoopNativeLibrary = hadoopHome + "/lib/native";

		// conf.set("tmpJars", HadoopNativeLibrary + ",
		// -Djava.library.path="+HadoopNativeLibrary);

		// @SuppressWarnings("unused")
		// int runTool = Sqoop.runTool(importArgsArr, conf);

		// 16-Oct-2017

		// return 0;

		return tool.run(options);
	}

	private static int performHdfsImport() {

		SqoopTool tool = new ImportTool();
		/*String[] args = { "--driver", "com.mysql.jdbc.Driver", 
				"--connect", "jdbc:mysql://192.168.1.224:3306/test_ananya", 
				"--username", "karadonga",
				"--password", "Karadonga1", 
				"--table", "employee", 
				"--target-dir", "test/broap" }; Working Partially*/
		
		String hadoopHome = System.getenv("HADOOP_HOME");
		String hdpMapRedHome = hadoopHome + "/share/hadoop/mapreduce";
		
		String[] args = { 
				"--driver", "com.mysql.jdbc.Driver",
				"--hadoop-home", hadoopHome,
				"--hadoop-mapred-home", hdpMapRedHome,
				
				"--connect", "jdbc:mysql://192.168.1.224:3306/test_ananya", 
				"--username", "karadonga",
				"--password", "Karadonga1", 
				"--table", "employee", 
				"--target-dir", UUID.randomUUID().toString()+"test/broaap" }; 

		SqoopOptions options = new SqoopOptions();

		try {
			options = tool.parseArguments(args, null, options, false);
			tool.validateOptions(options);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		tool.run(options);

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

		
		 setUp(); 
		 String tableName = "employee";
		 String tableNameInPosGresql = "mdm_incident_hi_detail";
		 TransferringEntireTableSpecificDirHive(tableNameInPosGresql,"result/data/"+ UUID.randomUUID().toString()); 
		 runIt();
		 

		/**
		 * Load Entire table
		 */

		/*
		 * System.out.println(SqoopOptions.getHadoopMapRedHome());
		 * 
		 * System.out.println(System.getenv("HADOOP_COMMON_LIB_NATIVE_DIR"));
		 * System.out.println(System.getenv("HADOOP_OPTS"));
		 * 
		 * setUp(); String tableName = "employee";
		 * TransferringEntireTable(tableName); runIt();
		 */

	}

}
