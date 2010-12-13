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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.client.ccindex.IndexedTableDescriptor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion;

import org.apache.hadoop.hbase.util.Bytes;

import com.sun.org.apache.xml.internal.serialize.Method;

class IndexedRegion extends TransactionalRegion {

	private static final Log LOG = LogFactory.getLog(IndexedRegion.class);
	private final HBaseConfiguration conf;
	private final IndexedTableDescriptor indexTableDescriptor;
	private static Map<IndexSpecification, HTable> indexSpecToTable[] = IndexedRegionServer.indexSpecToTables;
	private static Map<IndexSpecification, HTable> indexSpecToCCTS[] = IndexedRegionServer.indexSpecToCCTS;
	private static Map<String, HTable> tableToCCTS=IndexedRegionServer.tableToCCTS;
	private HTable orgTable = null;
	private static Object lockForCheck = new Object();


	public static final byte[] NO_VAlUE_INDEX = Bytes.toBytes("_NO_VALUE_");

	//private static HashMap<Long, ThreadPool> IndexUpdaterPool = new HashMap<Long, ThreadPool>();

	private static int regionN = 0;

	public IndexedRegion(final Path basedir, final HLog log,
			final FileSystem fs, final HBaseConfiguration conf,
			final HRegionInfo regionInfo, final FlushRequester flushListener,
			Leases trxLeases) throws IOException {
		super(basedir, log, fs, conf, regionInfo, flushListener, trxLeases);
		this.indexTableDescriptor = new IndexedTableDescriptor(regionInfo
				.getTableDesc());
		if(IndexedRegionServer.indexTableDescriptorMap.get(this.indexTableDescriptor.getBaseTableDescriptor().getNameAsString())==null)
		{
			IndexedRegionServer.
			indexTableDescriptorMap.
			put(this.indexTableDescriptor.getBaseTableDescriptor().getNameAsString(),
					this.indexTableDescriptor);
		}

		Collection<IndexSpecification> c = this.getIndexes();
		this.conf = conf;
		// for(IndexSpecification index:c)
		// this.getIndexTable(index);
	}


	static int tflag = 0;
	static int  cctflag=0;
	public static synchronized ConcurrentHashMap<IndexSpecification, HTable> getTableMap() {
		tflag++;
		if (tflag >= IndexedRegionServer.jobQueue) {
			tflag = 0;
		}
		return (ConcurrentHashMap<IndexSpecification, HTable>) indexSpecToTable[tflag];
	}
	public static synchronized ConcurrentHashMap<IndexSpecification, HTable> getCCTMap() {
		cctflag++;
		if (cctflag >= IndexedRegionServer.jobQueue) {
			cctflag = 0;
		}
		return (ConcurrentHashMap<IndexSpecification, HTable>) indexSpecToCCTS[cctflag];
	}
	private synchronized HTable getCCTTable(IndexSpecification index)
	throws IOException {
		ConcurrentHashMap<IndexSpecification, HTable> tMap = this.getCCTMap();
		HTable indexTable = tMap.get(index);
		if (indexTable == null) {
			indexTable = new HTable(conf, Bytes.add(index.getIndexedTableName(
					super
							.getRegionInfo().getTableDesc().getName()),CCIndexAdmin.CCT_TAIL)
					);
			tMap.put(index, indexTable);

		}
		indexTable.setWriteBufferSize(1000 * 1000 * 100);
		return indexTable;
	}
	
	private synchronized HTable getIndexTable(IndexSpecification index)
	throws IOException {
		ConcurrentHashMap<IndexSpecification, HTable> tMap = this.getTableMap();
		HTable indexTable = tMap.get(index);
		if (indexTable == null) {
			indexTable = new HTable(conf, index.getIndexedTableName(super
					.getRegionInfo().getTableDesc().getName()));
			tMap.put(index, indexTable);

		}
		indexTable.setWriteBufferSize(1000 * 1000 * 100);
		return indexTable;
	}
	private synchronized HTable getCCT(byte[] name)
	throws IOException {
	
		HTable indexTable =this.tableToCCTS.get(new String(name));
		if (indexTable == null) {
			indexTable = new HTable(conf, Bytes.add(name, CCIndexAdmin.CCT_TAIL));
			this.tableToCCTS.put(new String(name), indexTable);

		}
		indexTable.setWriteBufferSize(1000 * 1000 * 100);
		return indexTable;
	}

	private Collection<IndexSpecification> getIndexes() {
		return indexTableDescriptor.getIndexes();
	}

	/**
	 * @param batchUpdate
	 * @param lockid
	 * @param writeToWAL
	 *            if true, then we write this update to the log
	 * @throws IOException
	 */

	public void put(Put put, Integer lockId, boolean writeToWAL)

	throws IOException {
		Put putCCT=this.deleteAllIndexesMutiTable(put, lockId);
		if(putCCT!=null)
		{
			
			this.getCCT(super.getTableDesc().getName()).put(putCCT);
		}
		super.put(put, lockId, writeToWAL);
	}

	public static long deletTime = 0;
	public static long addTime = 0;
	public static long timeD = 0;
	public static long totalCallTime;

	private Put deleteAllIndexesMutiTable(Put put,Integer lockId) {
		boolean haveIndex=false;
		NavigableMap<byte[], byte[]> newColumnValues = getColumnsFromPut(put);
		for (IndexSpecification index : getIndexes()) {
			haveIndex=true;
			
			if (index != null && put.getRow() != null) {
				timeD++;
				totalCallTime++;
				if (timeD > 1000) {

					timeD = 0;
					System.out.println("deletTime:" + deletTime);
					System.out.println("addTime:" + addTime);
					System.out.println("totalCall:" + totalCallTime);
					System.out.println("deletTimes:" + deletTimes);
					// System.out.println("addTime:"+addTime);
					System.out.println("updateTimes:" + updateTimes);
				}

				long start = System.currentTimeMillis();
				Result d = null;
				// System.out.println("some thing wrong!");
				try {
					d = this.getOldValueInBaseTable(this, index, put.getRow(),
							newColumnValues, lockId);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				deletTime += (System.currentTimeMillis() - start);
				start = System.currentTimeMillis();
				IndexedRegionServer.addTask(this, index, put.getRow(),
						newColumnValues, lockId, d);
				addTime += (System.currentTimeMillis() - start);
			}
		}
		if(haveIndex)
			return IndexMaintenanceUtils.createBaseCCTUpdate(put.getRow(),newColumnValues,this.indexTableDescriptor);
		else
			return null;
	}

	/** Return the columns needed for the update. */
	private NavigableSet<byte[]> getColumnsForIndexes(
			Collection<IndexSpecification> indexes) {
		NavigableSet<byte[]> neededColumns = new TreeSet<byte[]>(
				Bytes.BYTES_COMPARATOR);
		for (IndexSpecification indexSpec : indexes) {
			for (byte[] col : indexSpec.getAllColumns()) {
				neededColumns.add(col);
			}
		}
		return neededColumns;
	}

	private void removeOldIndexEntry(IndexSpecification indexSpec, byte[] row,
			SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
		for (byte[] indexedCol : indexSpec.getIndexedColumns()) {
			if (!oldColumnValues.containsKey(indexedCol)) {
				LOG.debug("Index [" + indexSpec.getIndexId()
						+ "] not trying to remove old entry for row ["
						+ Bytes.toString(row) + "] because col ["
						+ Bytes.toString(indexedCol) + "] is missing");
				return;
			}
		}

		byte[] oldIndexRow = indexSpec.getKeyGenerator().createIndexKey(row,
				oldColumnValues);
		LOG.debug("Index [" + indexSpec.getIndexId() + "] removing old entry ["
				+ Bytes.toString(oldIndexRow) + "]");
		getIndexTable(indexSpec).delete(new Delete(oldIndexRow));
	}

	private NavigableMap<byte[], byte[]> getColumnsFromPut(Put put) {
		NavigableMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);
		for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
			for (KeyValue kv : familyPuts) {
				columnValues.put(kv.getColumn(), kv.getValue());
			}
		}
		return columnValues;
	}

	/**
	 * Ask if this put *could* apply to the index. It may actually apply if some
	 * of the columns needed are missing.
	 * 
	 * @param indexSpec
	 * @param put
	 * @return true if possibly apply.
	 */
	private boolean possiblyAppliesToIndex(IndexSpecification indexSpec, Put put) {
		for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
			for (KeyValue kv : familyPuts) {
				if (indexSpec.containsColumn(kv.getColumn())) {
					return true;
				}
			}
		}

		return false;
	}

	public Result superGet(Get get, Integer lockId)

	{
		try {
			return super.get(get, lockId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// FIXME: This call takes place in an RPC, and requires an RPC. This makes
	// for
	// a likely deadlock if the number of RPCs we are trying to serve is >= the
	// number of handler threads.
	static int deletTimes = 0;
	static int updateTimes = 0;

	public static Result getOldValueInBaseTable(IndexedRegion r,
			IndexSpecification indexSpec, byte[] row,
			SortedMap<byte[], byte[]> columnValues, Integer lockId)
	throws IOException {
		if (r.orgTable == null) {
			HTableDescriptor td = r.getTableDesc();

			r.orgTable = new HTable(r.conf, td.getName());
			r.orgTable.setWriteBufferSize(1000 * 1000 * 100);
		}
		HashSet<String> IndexColumns = new HashSet<String>();
		for (byte[] column : indexSpec.getIndexedColumns()) {
			IndexColumns.add(new String(column));
		}
		for (byte[] column : columnValues.keySet())
			if (IndexColumns.contains(new String(column))) {
				Get oldGet = new Get(row);
				oldGet.addColumn(column);
				Result oldResult = null;
				if (r.isClosed()) {
					oldResult = r.orgTable.get(oldGet);
				} else
					oldResult = r.superGet(oldGet, lockId);
				return oldResult;

			}
		return null;
	}
long timeAdelete=0;
	public static void putIndexMultTable(IndexedRegion r,
			IndexSpecification indexSpec, byte[] row,
			SortedMap<byte[], byte[]> columnValues, Integer lockId,
			Result oldResult) throws IOException {
		updateTimes++;
		HashSet<String> IndexColumns = new HashSet<String>();
		Put lastPut = null;

		if (r.orgTable == null) {
			HTableDescriptor td = r.getTableDesc();
			r.orgTable = new HTable(r.conf, td.getName());
			r.orgTable.setWriteBufferSize(1000 * 1000 * 20);
		}
	
		
		for (byte[] column : indexSpec.getIndexedColumns()) {
			IndexColumns.add(new String(column));
		}
		boolean hasIndex = false;

		for (byte[] column : columnValues.keySet())
			if (IndexColumns.contains(new String(column))) {
				if (oldResult != null && oldResult.raw() != null
						&& oldResult.list() != null) {
					if (oldResult.getValue(column).equals(
							columnValues.get(column))) {
						continue;
					}
					SortedMap<byte[], byte[]> oldColumnValues = r
					.convertToValueMap(oldResult);
					Get get = new Get(indexSpec.getKeyGenerator()
							.createIndexKey(row, oldColumnValues));
				
					get.addColumn(column);
					try {
						if (r.isClosed()) {
							oldResult = r.orgTable.get(get);
						} else
							oldResult = r.superGet(get, lockId);
						if (oldResult != null && oldResult.raw() != null
								&& oldResult.list() != null) {
							Delete delete = new Delete(get.getRow());
							r.getIndexTable(indexSpec).delete(delete);
							r.getCCTTable(indexSpec).delete(delete);
						}
					
						deletTimes++;
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

				}
			}

		for (byte[] column : columnValues.keySet())
			if (IndexColumns.contains(new String(column))) {

				hasIndex = true;
				Get oldGet = new Get(row);

				HColumnDescriptor dess[] = r.getTableDesc().getColumnFamilies();
				for (HColumnDescriptor des : dess) {
					if (columnValues.get(des.getName()) == null) {
						oldGet.addColumn(des.getName());
					}
				}
				// oldGet.addColumn(column);
				oldResult = null;
				if (r.isClosed()) {
					oldResult = r.orgTable.get(oldGet);
				} else
					oldResult = r.superGet(oldGet, lockId);
				if (oldResult != null && oldResult.raw() != null
						&& oldResult.list() != null) {
					for (KeyValue kv : oldResult.list()) {
						if (columnValues.get(kv.getColumn()) == null)
							columnValues.put(kv.getColumn(), kv.getValue());
					}
				}
			}
		if (!hasIndex) {
			Get oldGet = new Get(row);
			oldGet.addColumn(indexSpec.getIndexedColumns()[0]);
			oldResult = null;
			if (r.isClosed()) {
				oldResult = r.orgTable.get(oldGet);
			} else
				oldResult = r.superGet(oldGet, lockId);
			if (!(oldResult != null && oldResult.raw() != null && oldResult
					.list() != null)) {
				return;
			} else {
				SortedMap<byte[], byte[]> oldColumnValues = r
				.convertToValueMap(oldResult);
				for (KeyValue kv : oldResult.list()) {
					columnValues.put(kv.getColumn(), kv.getValue());
				}
			}

		}
		lastPut = IndexMaintenanceUtils.createIndexUpdate(indexSpec, row,
				columnValues);

		
		r.getIndexTable(indexSpec).put(lastPut);
		HTable cct=r.getCCTTable(indexSpec);
		cct.put(IndexMaintenanceUtils.createCCTUpdate(indexSpec, row,
				columnValues,r.indexTableDescriptor));

	}

	static private long timeForGet = 0;

	static private long timeForPut = 0;

	static private long timesForGet = 0;

	static private long timesForPut = 0;

	static private long addColumn = 0;

	static private int newRow = 0;

	static private int sameRow = 0;

	static private long timeForDelete = 0;

	static private int timesForDelete = 0;

	static private long callTimes = 0;

	private void updateIndex(IndexSpecification indexSpec, byte[] row,
			SortedMap<byte[], byte[]> columnValues) throws IOException {
		Put indexUpdate = IndexMaintenanceUtils.createIndexUpdate(indexSpec,
				row, columnValues);
		getIndexTable(indexSpec).put(indexUpdate);
		LOG.debug("Index [" + indexSpec.getIndexId() + "] adding new entry ["
				+ Bytes.toString(indexUpdate.getRow()) + "] for row ["
				+ Bytes.toString(row) + "]");

	}

	@Override
	public void delete(Delete delete, final Integer lockid, boolean writeToWAL)
	throws IOException {
		// First remove the existing indexes.
		if (!getIndexes().isEmpty()) {
			// Need all columns
			NavigableSet<byte[]> neededColumns = getColumnsForIndexes(getIndexes());

			Get get = new Get(delete.getRow());
			for (byte[] col : neededColumns) {
				get.addColumn(col);
			}

			Result oldRow = super.get(get, lockid);
			SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldRow);

			for (IndexSpecification indexSpec : getIndexes()) {
				removeOldIndexEntry(indexSpec, delete.getRow(), oldColumnValues);
			}
		}

		super.delete(delete, lockid, writeToWAL);

		if (!getIndexes().isEmpty()) {
			Get get = new Get(delete.getRow());

			// Rebuild index if there is still a version visible.
			Result currentRow = super.get(get, lockid);
			if (!currentRow.isEmpty()) {
				SortedMap<byte[], byte[]> currentColumnValues = convertToValueMap(currentRow);
				for (IndexSpecification indexSpec : getIndexes()) {
					if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec,
							currentColumnValues)) {
						updateIndex(indexSpec, delete.getRow(),
								currentColumnValues);
					}
				}
			}
		}
	}

	public static SortedMap<byte[], byte[]> convertToValueMap(Result result) {
		SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);

		if (result == null || result.raw() == null) {
			return currentColumnValues;
		}
		List<KeyValue> list = result.list();
		if (list != null) {
			for (KeyValue kv : result.list()) {
				currentColumnValues.put(kv.getColumn(), kv.getValue());
			}
		}
		return currentColumnValues;
	}

}
