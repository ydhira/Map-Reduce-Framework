package edu.cmu.qatar.cs214.hw.hw5.plugin.wordprefix;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import edu.cmu.qatar.cs214.hw.hw5.Emitter;
import edu.cmu.qatar.cs214.hw.hw5.MapTask;

/**
 * The map task for a word-prefix map/reduce computation.
 */
public class WordPrefixMapTask implements MapTask {
    private static final long serialVersionUID = 3046495241158633404L;

    @Override
    public void execute(InputStream in, Emitter emitter) {
    	Scanner scan = new Scanner (in);
    	scan.useDelimiter("\\W+");
    	while (scan.hasNext()){
    		String value = scan.next().trim().toLowerCase();
    		String key = "";
    		for (char c : value.toCharArray()) {
            	key = key + c;
            	try {
					emitter.emit(key, value);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
    	}
    	scan.close();
    }

}
