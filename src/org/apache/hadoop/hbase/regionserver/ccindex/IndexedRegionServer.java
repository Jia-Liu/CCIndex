/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.ccindex;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.client.ccindex.IndexedTableDescriptor;
import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.IndexedRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.hash.Hash;

/**
 * RegionServer which maintains secondary indexes.
 * 
 **/
public class IndexedRegionServer extends TransactionalRegionServer implements
		IndexedRegionInterface {

	public static ConcurrentHashMap<Integer, java.util.concurrent.ConcurrentLinkedQueue> taskQueue = new ConcurrentHashMap<Integer, java.util.concurrent.ConcurrentLinkedQueue>();
	public static ConcurrentHashMap<Integer, Worker> workers = new ConcurrentHashMap<Integer, Worker>();
	public static ConcurrentHashMap<WorkSet, Integer> refineSet = new ConcurrentHashMap<WorkSet, Integer>();
	private static int workerN = 20;
	private static int putIntver = 20;
	private static int putFlag = 0;
	private static int idleWorker = 0;
	public static int jobQueue = 8;
	public static int sequence = 0;
	public static ConcurrentHashMap<IndexSpecification, HTable> indexSpecToTables[];
	public static ConcurrentHashMap<IndexSpecification, HTable> indexSpecToCCTS[];
	public static ConcurrentHashMap<String, HTable> tableToCCTS;
	public static ConcurrentHashMap<String, IndexedTableDescriptor> indexTableDescriptorMap;

	// public static Map<IndexSpecification, HTable> indexSpecToTable = new
	// HashMap<IndexSpecification, HTable>();
	public class RegionFlusher extends Thread {
		HRegionServer server;
		int sleepN = 150;

		public RegionFlusher(HRegionServer server) {
			this.server = server;
		}

		public void run() {
			while (!this.server.stopRequested.get()) {

				for (HRegion region : this.server.getOnlineRegions()) {
					try {
						region.flushcache();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					for (int i = 0; i < this.sleepN
							&& (!this.server.stopRequested.get()); i++)
						this.sleep(1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	/**
	 * flush all regions to HDFS
	 */
	public static void flushTable() {
		for (int i = 0; i < jobQueue; i++) {
			for (HTable table : indexSpecToTables[i].values())

			{
				try {
					// System.out.println("begain flush table!");
					table.flushCommits();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (HTable table : indexSpecToCCTS[i].values())

			{
				try {
					// System.out.println("begain flush table!");
					table.flushCommits();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		for (HTable table : tableToCCTS.values())

		{
			try {
				// System.out.println("begain flush table!");
				table.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public IndexedRegionServer(HBaseConfiguration conf) throws IOException {
		super(conf);
		this.init();
	}

	public void init() {
		for (int i = 0; i < this.workerN; i++) {
			java.util.concurrent.ConcurrentLinkedQueue queue = new java.util.concurrent.ConcurrentLinkedQueue();
			taskQueue.put(i, queue);
			workers.put(i, new Worker(queue, this));
			workers.get(i).start();
		}
		indexSpecToTables = new ConcurrentHashMap[jobQueue];
		indexSpecToCCTS = new ConcurrentHashMap[jobQueue];
		tableToCCTS = new ConcurrentHashMap<String, HTable>();
		indexTableDescriptorMap = new ConcurrentHashMap<String, IndexedTableDescriptor>();
		for (int i = 0; i < this.jobQueue; i++) {
			indexSpecToTables[i] = new ConcurrentHashMap<IndexSpecification, HTable>();
			indexSpecToCCTS[i] = new ConcurrentHashMap<IndexSpecification, HTable>();
		}
		CheckerMaster master = new CheckerMaster(conf,
				this.indexTableDescriptorMap, this);
		master.start();
		RegionFlusher flusher = new RegionFlusher(this);
		flusher.start();
	}

	static int workerF = 0;

	public synchronized static int getIdleWorker() {
		workerF++;
		if (workerF >= workerN) {
			workerF = 0;
		}
		return workerF;

	}

	public static void addTask(IndexedRegion r, IndexSpecification indexSpec,
			byte[] row, SortedMap<byte[], byte[]> columnValues, Integer lockId,
			Result d) {

		idleWorker = getIdleWorker();
		synchronized (refineSet) {
			WorkSet set = new WorkSet(r, indexSpec, row, columnValues, lockId,
					d, sequence++);
			taskQueue.get(idleWorker).add(set);
			refineSet.put(set, set.sequence);
		}
	}

	static public class Worker extends Thread {
		long sleepTime[] = { 100, 100, 300, 300, 500, 1000, 3000 };
		java.util.concurrent.ConcurrentLinkedQueue<WorkSet> queue;
		int flag = 0;
		HRegionServer server;
		boolean free = false;

		public Worker(ConcurrentLinkedQueue queue, HRegionServer server) {
			this.server = server;
			this.queue = queue;
		}

		public void run() {
			while (!this.server.stopRequested.get()) {
				try {

					if (!queue.isEmpty()) {
						this.free = false;
						flag = 0;
						WorkSet set = queue.poll();
						synchronized (refineSet) {
							if (set.sequence != refineSet.get(set)) {
								System.out.println("cache hits");
								continue;
							} else {
								refineSet.remove(set);
							}
						}
						// set.getR().deleteIndexMultTable(set.getR(),
						// set.getIndexSpec(), set.getRow(),
						// set.getColumnValuIndexedRegionServer.workerses(),
						// set.getLockId());
						set.getR().putIndexMultTable(set.getR(),
								set.getIndexSpec(), set.getRow(),
								set.getColumnValues(), set.getLockId(), set.d);

					} else {
						try {
							this.free = true;
							if (flag >= this.sleepTime.length - 1) {

								flushTable();

								// System.out.println(this.getId()
								// + ":there is no jobs");
								sleep(this.sleepTime[flag]);
							} else {
								// flushTable();
								sleep(this.sleepTime[flag++]);
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					continue;
				}
			}
		}
	}

	static public class WorkSetQueue {
		WorkSet head = null;
		WorkSet tail = null;
		int minBuffer = 50;
		int size = 0;
		Lock lock = new ReentrantLock();

		public int size() {
			return this.size;
		}

		public void add(WorkSet w) {

			if (w == null)
				return;

			if (this.size > this.minBuffer) {

				this.size++;
				if (tail == null) {

					this.tail = w;
					this.head = w;

				} else {
					tail.next = w;
					tail = w;
				}

			} else {
				this.lock.lock();
				this.size++;
				if (tail == null) {

					this.tail = w;
					this.head = w;

				} else {
					tail.next = w;
					tail = w;
				}
				this.lock.unlock();
			}

		}

		public WorkSet poll() {
			if (this.size > this.minBuffer) {

				if (head == null) {
					this.size = 0;

					return head;
				} else {
					WorkSet ret = head;
					head = ret.next;
					this.size--;
					if (head == null) {
						tail = null;
					}

					return ret;
				}

			} else {
				this.lock.lock();
				if (head == null) {
					this.size = 0;
					this.lock.unlock();

					return head;
				} else {
					WorkSet ret = head;
					head = ret.next;
					this.size--;
					if (head == null) {
						tail = null;
					}
					this.lock.unlock();
					return ret;

				}
			}

		}
	}

	static public class WorkSet {

		int sequence = 0;

		Hash hash = Hash.getInstance(Hash.JENKINS_HASH);;
		byte[] ID;

		public boolean equals(Object arg0) {

			if (arg0 instanceof WorkSet) {
				WorkSet arg = (WorkSet) arg0;
				if (this.indexSpec.equals(arg.indexSpec)
						&& Bytes.equals(this.row, arg.row))
					return true;
				else
					return false;
			}
			return false;
		}

		public int hashCode() {
			if (this.ID == null)
				this.ID = Bytes.add(row, indexSpec.getIndexId().getBytes());

			if (hash == null) {
				System.out.println("null hash");
			} else if (this.ID == null) {
				System.out.println("null.eslse");
			}
			return hash.hash(this.ID);
		}

		IndexedRegion r;
		IndexSpecification indexSpec;
		byte[] row;
		SortedMap<byte[], byte[]> columnValues;
		Integer lockId;

		WorkSet next = null;
		Result d = null;

		public WorkSet(IndexedRegion r, IndexSpecification indexSpec,
				byte[] row, SortedMap<byte[], byte[]> columnValues,
				Integer lockId, Result d, int seq) {
			this.r = r;
			this.indexSpec = indexSpec;
			this.row = row;
			this.columnValues = columnValues;
			this.lockId = lockId;
			this.d = d;
			this.sequence = seq;
			this.ID = Bytes.add(row, indexSpec.getIndexId().getBytes());
		}

		public SortedMap<byte[], byte[]> getColumnValues() {
			return columnValues;
		}

		public void setColumnValues(SortedMap<byte[], byte[]> columnValues) {
			this.columnValues = columnValues;
		}

		public IndexSpecification getIndexSpec() {
			return indexSpec;
		}

		public void setIndexSpec(IndexSpecification indexSpec) {
			this.indexSpec = indexSpec;
		}

		public Integer getLockId() {
			return lockId;
		}

		public void setLockId(Integer lockId) {
			this.lockId = lockId;
		}

		public WorkSet getNext() {
			return next;
		}

		public void setNext(WorkSet next) {
			this.next = next;
		}

		public IndexedRegion getR() {
			return r;
		}

		public void setR(IndexedRegion r) {
			this.r = r;
		}

		public byte[] getRow() {
			return row;
		}

		public void setRow(byte[] row) {
			this.row = row;
		}

	}

	@Override
	public long getProtocolVersion(final String protocol,
			final long clientVersion) throws IOException {
		if (protocol.equals(IndexedRegionInterface.class.getName())) {
			return HBaseRPCProtocolVersion.versionID;
		}
		return super.getProtocolVersion(protocol, clientVersion);
	}

	@Override
	protected HRegion instantiateRegion(final HRegionInfo regionInfo)
			throws IOException {
		HRegion r = new IndexedRegion(HTableDescriptor.getTableDir(super
				.getRootDir(), regionInfo.getTableDesc().getName()),
				super.hlog, super.getFileSystem(), super.conf, regionInfo,
				super.getFlushRequester(), super.getTransactionalLeases());
		r.initialize(null, new Progressable() {
			public void progress() {
				addProcessingMessage(regionInfo);
			}
		});
		return r;
	}

	

	

}
