package org.apache.hadoop.hbase.regionserver.ccindex;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.IndexedTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;



public class CheckerMaster extends Thread {
	long sleeptime = 260000;
	ConcurrentHashMap<String, IndexedTableDescriptor> indexTableDescriptorMap;
	private final HBaseConfiguration conf;
	int flag = 0;
	HRegionServer server;

	public CheckerMaster(

			HBaseConfiguration conf,
			ConcurrentHashMap<String, IndexedTableDescriptor> indexTableDescriptorMap,
			HRegionServer server) {
		this.conf = conf;
		this.server = server;
		this.indexTableDescriptorMap = indexTableDescriptorMap;

	}

	private void checkTable(IndexedTableDescriptor des) {
		if (des.getIndexes().size() == 0)
			return;
		Checker[] checkers = new Checker[des.getIndexes().size() + 1];
		checkers[0] = new Checker(conf, des.getBaseTableDescriptor()
				.getName(), des, this.server);
		int i = 1;
		for (IndexSpecification index : des.getIndexes()) {
			checkers[i] = new Checker(conf, index.getIndexedTableName(des
					.getBaseTableDescriptor().getName()), des, this.server);
			i++;
		}
		for (int j = 0; j < checkers.length; j++) {
			checkers[j].start();
		}
		for (int j = 0; j < checkers.length; j++) {
			try {
				checkers[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void run() {
		while (!this.server.stopRequested.get()) {
			try {

				sleep(this.sleeptime);
				int size = IndexedRegionServer.workers.size();
				int freeN = 0;
				for (IndexedRegionServer.Worker w : IndexedRegionServer.workers.values()) {
					if (w.free) {
						freeN++;
					}
				}

				if (freeN > size / 2)
					for (String key : this.indexTableDescriptorMap.keySet()) {

						IndexedTableDescriptor des = this.indexTableDescriptorMap
								.get(key);
						this.checkTable(des);

					}

			} catch (Exception e) {
				continue;
			}
		}
	}
}