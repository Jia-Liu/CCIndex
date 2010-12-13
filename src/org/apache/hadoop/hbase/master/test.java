package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class test {
	public static void main(String args[])
	{
		test t =new test();
		t.processDeadServerRegionInMeta();

	}
	public void processDeadServerRegionInMeta() {
		HTable metaTable;
		Scan indexScan = new Scan();
		try {
			HBaseConfiguration conf = new HBaseConfiguration();
			metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
			metaTable.setAutoFlush(false);
			metaTable.setWriteBufferSize(1024 * 1024 * 12);
			metaTable.setScannerCaching(1000);
		//	Scan indexScan = new Scan();
			indexScan.setCaching(1000);
			ResultScanner indexScanner = metaTable.getScanner(indexScan);
			Result r = null;
			
	
			while ((r = indexScanner.next()) != null) {
				//LOG.debug("start scan meta table:"+r);
				//System.out.println(r);
				try {
					byte[] value = r.getValue(HConnectionManager.CATALOG_FAMILY,
							HConnectionManager.REGIONINFO_QUALIFIER);
					value = r.getValue(HMaster.CATALOG_FAMILY, HMaster.REGIONINFO_QUALIFIER);
					if (value == null || value.length == 0) {
						throw new IOException("HRegionInfo was null or empty in ");
					}
					// convert the row result into the HRegionLocation we need!
					HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
							value, new HRegionInfo());
					// possible we got a region of a different table...

					System.out.println(regionInfo);
		
					value = r.getValue(HMaster.CATALOG_FAMILY, HMaster.SERVER_QUALIFIER);
					String serverAddress = "";
					if (value != null) {
						serverAddress = Bytes.toString(value);
					}
					if (serverAddress.equals("")) {
						throw new NoServerForRegionException(
								"No server address listed " + "in ");
					}

					// instantiate the location
					HRegionLocation location = new HRegionLocation(regionInfo,
							new HServerAddress(serverAddress));
		
					serverAddress = BaseScanner.getServerAddress(r);
					long startCode = BaseScanner.getStartCode(r);

					String serverName = null;
					if (serverAddress != null && serverAddress.length() > 0) {
						serverName = HServerInfo.getServerName(serverAddress,
								startCode);
					}

				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Throwable e) {

		}


	}
}
