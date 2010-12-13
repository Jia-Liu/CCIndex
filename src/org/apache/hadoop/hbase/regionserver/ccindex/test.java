package org.apache.hadoop.hbase.regionserver.ccindex;

import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.util.Bytes;

public class test {
	public static void main(String arg[])
	{
		String s="0000000335000000033500000010";
		byte sb[]=Bytes.toBytes(s);
		SimpleIndexKeyGenerator.getOrgRowKey(sb);
	}

}
