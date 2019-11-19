package com.upgrad.trendproject.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TrendPojo implements Writable{

	private Text songId;
	private IntWritable date;
	
	public TrendPojo() {
		songId = new Text();
		date = new IntWritable();
	}

	public TrendPojo(Text songId, IntWritable date) {
		this.songId = songId;
		this.date = date;
	}
	
	public Text getSongId() {
		return songId;
	}
	public void setSongId(Text songId) {
		this.songId = songId;
	}
	public IntWritable getDate() {
		return date;
	}
	public void setDate(IntWritable date) {
		this.date = date;
	}
	
	public void write(DataOutput out) throws IOException {
		songId.write(out);
		date.write(out);
		
	}
	public void readFields(DataInput in) throws IOException {
		songId.readFields(in);
		date.readFields(in);
	}
	
	
	@Override
    public String toString() {
        return songId + " " + date;
    }
 
    @Override
    public int hashCode(){
        return songId.hashCode()*163 + date.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TrendPojo)
        {
            TrendPojo pojo = (TrendPojo) o;
            return songId.equals(pojo.songId) && date.equals(pojo.date);
        }
        return false;
    }
	
    public int compareTo(TrendPojo pojo) {
        int cmp = songId.compareTo(pojo.getSongId());
        if (cmp != 0) {
            return cmp;
        }
 
        return date.compareTo(pojo.getDate());
    }
	
	
}
