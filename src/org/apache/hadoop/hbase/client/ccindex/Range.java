package org.apache.hadoop.hbase.client.ccindex;

public class Range {
	private byte[] column;
	private byte[] start;
	private byte[] end;
	private byte[][] baseColumns;
	public byte[][] getBaseColumns() {
		return baseColumns;
	}
	public void setBaseColumns(byte[][] baseColumns) {
		this.baseColumns = baseColumns;
	}
	public byte[] getColumn() {
		return column;
	}
	public void setColumn(byte[] column) {
		this.column = column;
	}
	public byte[] getStart() {
		return start;
	}
	public void setStart(byte[] start) {
		this.start = start;
	}
	public byte[] getEnd() {
		return end;
	}
	public void setEnd(byte[] end) {
		this.end = end;
	}
	public String toString()
	{
		StringBuffer b=new StringBuffer();
		b.append("column is:");
		b.append(new String(this.column));
		b.append("start is:");
		if(start!=null)
		b.append(new String(this.start));
		else
			b.append("null");
		b.append("end is:");
		if(end!=null)
			b.append(new String(this.end));
			else
				b.append("null");
		return b.toString();
		
	}

}
