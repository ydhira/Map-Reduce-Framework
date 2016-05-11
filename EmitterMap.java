package edu.cmu.qatar.cs214.hw.hw5;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class EmitterMap implements Emitter{

	File file;
	FileOutputStream out;
	public EmitterMap(File file){
		this.file = file;
	}
	
	
	@Override
	public void close() throws IOException {
		out.close();
		
	}

	@Override
	public void emit(String key, String value) throws IOException {
		//create a file output stream and wrute the key value pair onto the file
		
		String keyvalue = key + " " + value + "\n";
		FileOutputStream out = new FileOutputStream(file, true);
		this.out = out;
		out.write(keyvalue.getBytes());
		out.close();
		
		
		
	}

	
	
}
