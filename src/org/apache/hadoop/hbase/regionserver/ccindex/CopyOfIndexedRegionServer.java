package org.apache.hadoop.hbase.regionserver.ccindex;
///**
// * Copyright 2009 The Apache Software Foundation
// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
///*
//package org.apache.hadoop.hbase.regionserver.tableindexed;
//
//import java.io.IOException;
//import java.lang.Thread.UncaughtExceptionHandler;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HRegionInfo;
//import org.apache.hadoop.hbase.HServerInfo;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.Leases;
//import org.apache.hadoop.hbase.NotServingRegionException;
//import org.apache.hadoop.hbase.RemoteExceptionHandler;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
//import org.apache.hadoop.hbase.ipc.IndexedRegionInterface;
//import org.apache.hadoop.hbase.regionserver.HLog;
//import org.apache.hadoop.hbase.regionserver.HRegion;
//import org.apache.hadoop.hbase.regionserver.HRegionServer;
//import org.apache.hadoop.hbase.regionserver.InternalScanner;
//
//import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion;
//import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegionServer;
//import org.apache.hadoop.hbase.util.Threads;
//import org.apache.hadoop.io.MapWritable;
//import org.apache.hadoop.util.Progressable;
//
///**
// * RegionServer which maintains secondary indexes.
// * 
// **/
//public class CopyOfIndexedRegionServer extends HRegionServer implements
//    IndexedRegionInterface {
//	static final Log LOG = LogFactory.getLog(TransactionalRegionServer.class);
//  public IndexedRegionServer(HBaseConfiguration conf) throws IOException {
//    super(conf);
//  }
//
//  
//  public long getProtocolVersion(final String protocol, final long clientVersion)
//      throws IOException {
//    if (protocol.equals(IndexedRegionInterface.class.getName())) {
//      return HBaseRPCProtocolVersion.versionID;
//    }
//    return super.getProtocolVersion(protocol, clientVersion);
//  }
//
//  
//  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
//      throws IOException {
//	
//    HRegion r =  new  IndexedRegion(super
//	        .getRootDir(),super.hlog, super
//	        .getFileSystem(), super.conf,regionInfo, super.getFlushRequester() );
//    	r.initialize(null, new Progressable() {
//      public void progress() {
//        addProcessingMessage(regionInfo);
//      }
//    });
//    return r;
//  }
//  protected void init(final MapWritable c) throws IOException {
//	    super.init(c);
//	    String n = Thread.currentThread().getName();
//	    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
//	      public void uncaughtException(final Thread t, final Throwable e) {
//	        abort();
//	        LOG.fatal("Set stop flag in " + t.getName(), e);
//	      }
//	    };
//
//	  }
//
//	  
//	  protected HLog instantiateHLog(Path logdir) throws IOException {
//	    HLog newlog =super.instantiateHLog(logdir);
//	    return newlog;
//	  }
//	  
//	  
//	  protected IndexedRegion getIndexedRegion(final byte[] regionName)
//	      throws NotServingRegionException {
//	    return (IndexedRegion) super.getRegion(regionName);
//	  }
//	  
//	
//
//	  /** We want to delay the close region for a bit if we have commit pending transactions.
//	   * 
//	   */
//	  
//	  protected void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
//	  throws IOException {
//	    super.closeRegion(hri, reportWhenCompleted);
//	  }
//	  
//	  public void abort(final byte[] regionName, final long transactionId)
//	      throws IOException {
//	
//	  }
//
//	  public void commit(final byte[] regionName, final long transactionId)
//	      throws IOException {
//		 
//	
//	  }
//
//
//	  
//
//
//	  public long openScanner(final long transactionId, byte [] regionName, Scan scan)
//	  throws IOException {
//	    checkOpen();
//	    NullPointerException npe = null;
//	    if (regionName == null) {
//	      npe = new NullPointerException("regionName is null");
//	    } else if (scan == null) {
//	      npe = new NullPointerException("scan is null");
//	    }
//	    if (npe != null) {
//	      throw new IOException("Invalid arguments to openScanner", npe);
//	    }
//	    super.getRequestCount().incrementAndGet();
//	    try {
//	    	IndexedRegion r = getIndexedRegion(regionName);
//	      InternalScanner s = r.getScanner( scan);
//	      long scannerId = addScanner(s);
//	      return scannerId;
//	    } catch (IOException e) {
//	      LOG.error("Error opening scanner (fsOk: " + this.fsOk + ")",
//	          RemoteExceptionHandler.checkIOException(e));
//	      checkFileSystem();
//	      throw e;
//	    }
//	  }
//
//	
//
//	  public void delete( byte[] regionName, Delete delete)
//	      throws IOException {
//		  getIndexedRegion(regionName).delete( delete, super.getMsgInterval(), super.abortRequested);
//	  }
//
//	  public Result get( byte[] regionName, Get get)
//	      throws IOException {
//	    return getIndexedRegion(regionName).get( get, super.getMsgInterval());
//	  }
//
//	  public void put( byte[] regionName, Put put)
//	      throws IOException {
//	    getIndexedRegion(regionName).put(put);
//	    
//	  }
//
//	  public int put( byte[] regionName, Put[] puts)
//	      throws IOException {
//		  for(Put put:puts)
//			  getIndexedRegion(regionName).put(put);
//	    return puts.length; // ??
//	  }
//
//
//
//
//
//	
//	public void close(long scannerId) throws IOException {
//		// TODO Auto-generated method stub
//		super.close(scannerId);
//		
//	}
//
//
//
//
//
//	
//	public int delete(byte[] regionName, Delete[] deletes) throws IOException {
//		// TODO Auto-generated method stub
//		for(int i=0;i<deletes.length;i++)
//		{
//			this.delete(regionName, deletes[i]);
//		}
//		return 0;
//	}
//
//
//
//
//
//
//
//
//	
//	public HRegion[] getOnlineRegionsAsArray() {
//		return super.getOnlineRegionsAsArray();
//		
//	}
//
//
//	
//	public HRegionInfo getRegionInfo(byte[] regionName)
//			throws NotServingRegionException {
//		return super.getRegionInfo(regionName);
//	
//	}
//
//
//	
//	public HRegionInfo[] getRegionsAssignment() throws IOException {
//		// TODO Auto-generated method stub
//		return super.getRegionsAssignment();
//	}
//
//
//	
//	public long incrementColumnValue(byte[] regionName, byte[] row,
//			byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
//			throws IOException {
//		return super.incrementColumnValue(regionName, row, family, qualifier, amount, writeToWAL);
//	
//	}
//
//
//	
//	public long lockRow(byte[] regionName, byte[] row) throws IOException {
//	
//		return super.lockRow(regionName, row);
//
//	}
//
//
//	
//	public Result next(long scannerId) throws IOException {
//		return super.next(scannerId);
//
//	}
//
//
//	
//	public Result[] next(long scannerId, int numberOfRows) throws IOException {
//		return super.next(scannerId,numberOfRows);
//	}
//
//
//	
//	public long openScanner(byte[] regionName, Scan scan) throws IOException {
//	    checkOpen();
//	    NullPointerException npe = null;
//	    if (regionName == null) {
//	      npe = new NullPointerException("regionName is null");
//	    } else if (scan == null) {
//	      npe = new NullPointerException("scan is null");
//	    }
//	    if (npe != null) {
//	      throw new IOException("Invalid arguments to openScanner", npe);
//	    }
//	    super.getRequestCount().incrementAndGet();
//	    try {
//	      IndexedRegion r = getIndexedRegion(regionName);
//	      InternalScanner s = r.getScanner( scan);
//	      long scannerId = addScanner(s);
//	      return scannerId;
//	    } catch (IOException e) {
//	      LOG.error("Error opening scanner (fsOk: " + this.fsOk + ")",
//	          RemoteExceptionHandler.checkIOException(e));
//	      checkFileSystem();
//	      throw e;
//	    }
//		
//	}
//
//
//
//
//
//
//
//
//
//
//	
//	public boolean commitIfPossible(byte[] regionName, long transactionId)
//			throws IOException {
//		// TODO Auto-generated method stub
//		//super.getRequestCount().incrementAndGet();
//		return false;
//	}
//
//
//	
//	public int commitRequest(byte[] regionName, long transactionId)
//			throws IOException {
//		// TODO Auto-generated method stub
//		//super.getRequestCount().incrementAndGet();
//		return 0;
//	}
//
//
//	
//	public void beginTransaction(long transactionId, byte[] regionName)
//			throws IOException {
//		// TODO Auto-generated method stub
//		
//	}
//
//
//	
//	public void delete(long transactionId, byte[] regionName, Delete delete)
//			throws IOException {
//		// TODO Auto-generated method stub
//		this.delete(regionName, delete);
//		
//	}
//
//
//	
//	public Result get(long transactionId, byte[] regionName, Get get)
//			throws IOException {
//		// TODO Auto-generated method stub
//		return this.get(regionName, get);
//		
//	}
//
//
//	
//	public void put(long transactionId, byte[] regionName, Put put)
//			throws IOException {
//		// TODO Auto-generated method stub
//		this.put(regionName, put);
//	}
//
//
//	
//	public int put(long transactionId, byte[] regionName, Put[] puts)
//			throws IOException {
//		return this.put(regionName, puts);
//
//	}
//
//}
