package com.hemendra.sqooptest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.tool.SqoopTool;

public class SqoopTest {
	public static void main(String[] args) {
		String actioname = "import-all-tables";
		Configuration conf = new Configuration();
		Configuration pluginConf = SqoopTool.loadPlugins(conf);
		SqoopTool tool = SqoopTool.getTool(actioname);
		Sqoop sqoop = new Sqoop(null, conf);
		String[] toolArgs;
		//toolArgs = sqoop.stashChildPrgmArgs(args);
		//int t = ToolRunner.run(sqoop.getConf(), sqoop, toolArgs);
		
	}

}
