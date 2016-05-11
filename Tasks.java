package edu.cmu.qatar.cs214.hw.hw5;

import java.io.Serializable;

public class Tasks implements Serializable {
	

	private static final long serialVersionUID = 4689811531262175592L;
	MapTask m;
	ReduceTask r;
	
	public Tasks(MapTask m, ReduceTask r){
		this.m = m;
		this.r = r;
	}
	
	public MapTask getMapTask(){
		return this.m;
	}
	
	public ReduceTask getReduceTask(){
		return this.r;
	}
	
}
