package com.example.Operator;

import java.io.IOException;
//import java.util.Map;
//import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class WordReaderTest {
	private final static String STOP_WORD_FILE_PATH = "src/test/resources/stop-words";
	private static WordCountOperator wordCountOperator = new WordCountOperator();
	private static WordReader wordReader = new WordReader();
	private static CollectorTestSink<Object> sink;
//	private static final Pattern nonWordDefault = Pattern.compile("[\\p{Punct}\\s]+");
	
	@Before
	public void setup() {
		  Path wd = null;
		  try {
		    FileSystem fs = FileSystem.get(new Configuration());
		    wd = fs.getWorkingDirectory();
		  } catch (IOException e) {
		      e.printStackTrace();
		  }
	
		  // Create a word count operator and call the setup method. setup() will actually be called by the Apache Apex engine during runtime.
		  wordCountOperator = new WordCountOperator();
		  wordReader = new WordReader();
		  String path = wd.toString()+"/"+STOP_WORD_FILE_PATH;
		  wordCountOperator.setStopWordFilePath(path);
		  wordCountOperator.setup(null);
	
		  // Create a dummy sink to simulate the output port and set it as the output port of the operator
		  sink = new CollectorTestSink<>();
		  wordCountOperator.output.setSink(sink);
	}
	
	@Test
	public void testWordCountPerTuple() {
		// Set the per tuple option to true
	    wordCountOperator.setSendPerTuple(true);

	    wordCountOperator.beginWindow(0);
	    wordCountOperator.input.process("Humpty Dumpy sat on a Wall");
	    wordCountOperator.input.process("Humpty Dumpy had a great fall");
	    wordCountOperator.endWindow();
	    
	    Assert.assertEquals(8, sink.collectedTuples.size());
	}
		  
	@Test
	public void testSetNonWordStr() {
		wordReader.setnonWordStr("hello World!");
		Assert.assertEquals("hello World!", wordReader.getnonWordStr());
		
	}
	
	@After
	public void teardown() {
		wordReader.teardown();
	}
	  
}
