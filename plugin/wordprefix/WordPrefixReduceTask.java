package edu.cmu.qatar.cs214.hw.hw5.plugin.wordprefix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import edu.cmu.qatar.cs214.hw.hw5.Emitter;
import edu.cmu.qatar.cs214.hw.hw5.ReduceTask;

/**
 * The reduce task for a word-prefix map/reduce computation.
 */
public class WordPrefixReduceTask implements ReduceTask {
    private static final long serialVersionUID = 6763871961687287020L;

    
    public void execute(String key, Iterator<String> values, Emitter emitter) throws IOException {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        while (values.hasNext()) {
        	String value = values.next();
        	if (result.containsKey(value)) {
        		result.put(value, result.get(value)+1);
        	} else {
        		result.put(value, 1);
        	}
        }
        HashMap.Entry<String, Integer> p = null;

        for (HashMap.Entry<String, Integer> entry : result.entrySet()){
        	if (p == null || entry.getValue().compareTo(p.getValue()) > 0){
        		p = entry;
        	}
        }
        String value = p.getKey();
        emitter.emit(key, value);
    } 

}
