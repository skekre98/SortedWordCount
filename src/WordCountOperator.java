package com.example.Operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Maps;

public class WordCountOperator extends BaseOperator {
	
	private boolean sendPerTuple = true; //Default
	private String stopWordFilePath;
	private transient String[] stopWords;
	private Map<String, Long> globalCounts;
	private transient Map<String, Long> updatedCounts;
	public transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		
		@Override
		public void process(String tuple) {
			processTuple(tuple);
		}
	};
	
	public transient DefaultOutputPort<Entry<String, Long>> output = new DefaultOutputPort<Entry<String, Long>>();
	
	public WordCountOperator() {
		globalCounts = Maps.newHashMap();
	}
	
	/* 
	 * Setup Call
	 */
	
	@Override
	public void setup(OperatorContext context) {
		String line = "";
		BufferedReader br = null;
		try{
		      Configuration conf = new Configuration();
		      FileSystem fs = FileSystem.get(conf);
		      Path filePath = new Path("/Users/sharvilkekre/Desktop/stop-words.txt");
		      br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		      StringBuilder stopWordText = new StringBuilder();
		      while((line = br.readLine()) != null)
		      {
		        stopWordText.append(line.toLowerCase()+"\n");
		      }
		      br.close();
		      stopWords = stopWordText.toString().split("[ \n]");
		    } 
		    catch (IOException e) {
		      throw new RuntimeException("Exception in reading stop word file", e);
		    }

		    // Initialize updatedCounts in setup call since setup may be called at the start of the operator lifetime or after a crash. Being a transient variable, it will lose all the data.
		    updatedCounts = Maps.newHashMap();
	}
	
	public void beginWindow(long windowId) {
		
		if (!sendPerTuple) {
			updatedCounts.clear();
		}
	}
	
	protected void processTuple(String tuple) {
	    if(sendPerTuple)
	    {
	      updatedCounts.clear();
	    }
	    String[] words = tuple.toLowerCase().split("[ ]");
	    for(String word: words) { 
	    	if (!Arrays.asList(stopWords).contains(word)) {
	    		if (globalCounts.containsKey(word)) {
		    		globalCounts.put(word, globalCounts.get(word) + 1);
		    	} else {
		    		globalCounts.put(word, 1L);
		    	}
	    		updatedCounts.put(word, globalCounts.get(word));
	    	}
	    }
	    
	    if (sendPerTuple) {
	    	for (Entry<String, Long> entry: updatedCounts.entrySet()) {
	    		output.emit(entry);
	    	}
	    }
	}
	
	@Override
	public void endWindow() {
		if (!sendPerTuple) {
			for (Entry<String, Long> entry: updatedCounts.entrySet()) {
				output.emit(entry);
			}
		}
	}
	
	public boolean isSendPerTuple() {
		
	    return sendPerTuple;
	    
	}

	public void setSendPerTuple(boolean sendPerTuple) {
		
		this.sendPerTuple = sendPerTuple;
	    
	}

	public String getStopWordFilePath() {
		  
		return stopWordFilePath;
		  
	 }

	public void setStopWordFilePath(String stopWordFilePath) {
		  
		this.stopWordFilePath = stopWordFilePath;
	    
	}
	
}
