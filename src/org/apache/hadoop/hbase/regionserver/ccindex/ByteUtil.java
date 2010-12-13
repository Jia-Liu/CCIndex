package org.apache.hadoop.hbase.regionserver.ccindex;


import java.nio.ByteBuffer; 

public class ByteUtil { 

    /** 
8.     * @param args 
9.     */ 
    public static void main(String[] args) { 
        short s = -20; 
        byte[] b = new byte[2]; 
        putShort(b, s, 0); 
        ByteBuffer buf = ByteBuffer.allocate(2); 
        buf.put(b); 
        buf.flip(); 
        System.out.println(getShort(b, 0)); 
        System.out.println(buf.getShort()); 
        System.out.println("***************************"); 
        int i = -40; 
        b = new byte[4]; 
        putInt(b, i, 0); 
        buf = ByteBuffer.allocate(4); 
        buf.put(b); 
        buf.flip(); 
        System.out.println(getInt(b, 0)); 
        System.out.println(buf.getInt()); 
        System.out.println("***************************"); 
        long l = -40; 
        b = new byte[8]; 
        putLong(b, l, 0); 
        buf = ByteBuffer.allocate(8); 
        buf.put(b); 
        buf.flip(); 
        System.out.println(getLong(b, 0)); 
        System.out.println(buf.getLong()); 
        System.out.println("***************************"); 
    } 

    public static void putShort(byte b[], short s, int index) { 
        b[index] = (byte) (s >> 8); 
        b[index + 1] = (byte) (s >> 0); 
    } 

    public static short getShort(byte[] b, int index) { 
        return (short) (((b[index] << 8) | b[index + 1] & 0xff)); 
    } 

    // /////////////////////////////////////////////////////// 
    public static void putInt(byte[] bb, int x, int index) { 
        bb[index + 0] = (byte) (x >> 24); 
        bb[index + 1] = (byte) (x >> 16); 
        bb[index + 2] = (byte) (x >> 8); 
        bb[index + 3] = (byte) (x >> 0); 
   } 

    public static int getInt(byte[] bb, int index) { 
        return (int) ((((bb[index + 0] & 0xff) << 24) 
                | ((bb[index + 1] & 0xff) << 16) 
                | ((bb[index + 2] & 0xff) << 8) | ((bb[index + 3] & 0xff) << 0))); 
    } 

    // ///////////////////////////////////////////////////////// 
    	public static byte[] subByte(byte[] bb,long begain,long end)
    	{
    		if(begain<0||begain>end||end>=bb.length)
    			return null;
    			else
    			{		
    				byte[] rt=new byte[(int)(end-begain+1)]; 
    				long i=begain;
    				for(;i<=end;i++)
    				{
    						rt[(int) (i-begain)]=bb[(int) i];
    				}
    				return rt;
    	
    			}
    	}
    public static void putLong(byte[] bb, long x, int index) { 
        bb[index + 0] = (byte) (x >> 56); 
        bb[index + 1] = (byte) (x >> 48); 
        bb[index + 2] = (byte) (x >> 40); 
        bb[index + 3] = (byte) (x >> 32); 
        bb[index + 4] = (byte) (x >> 24); 
        bb[index + 5] = (byte) (x >> 16); 
        bb[index + 6] = (byte) (x >> 8); 
        bb[index + 7] = (byte) (x >> 0); 
    } 
    public static byte[] putLong(long x)
    {
    	byte [] a=new byte[8];
    	 putLong(a,x,0);
    	 return a;
    	
    }

    public static long getLong(byte[] bb, int index) { 
        return ((((long) bb[index + 0] & 0xff) << 56) 
                | (((long) bb[index + 1] & 0xff) << 48) 
                | (((long) bb[index + 2] & 0xff) << 40) 
                | (((long) bb[index + 3] & 0xff) << 32) 
                | (((long) bb[index + 4] & 0xff) << 24) 
                | (((long) bb[index + 5] & 0xff) << 16) 
                | (((long) bb[index + 6] & 0xff) << 8) | (((long) bb[index + 7] & 0xff) << 0)); 
    } 
}