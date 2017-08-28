package com.example.Operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class LineReader extends AbstractFileInputOperator<String>{
	
	private static final Logger LOG = LoggerFactory.getLogger(LineReader.class);
	public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();

	@OutputPortFieldAnnotation(optional = true)
	public final transient DefaultOutputPort<String> control = new DefaultOutputPort<>();
	private transient BufferedReader br = null;
	private Path path;
	
	@Override
	protected InputStream openFile(Path curPath) throws IOException {
	  LOG.info("openFile: curPath = {}", curPath);
	  path = curPath;
	  InputStream is = super.openFile(path);
	  br = new BufferedReader(new InputStreamReader(is));
	  return is;
	}

	@Override
	protected void closeFile(InputStream is) throws IOException {
	  super.closeFile(is);
	  br.close();
	  br = null;
	  path = null; 
	}

	@Override
	protected String readEntity() throws IOException {
		final String line = br.readLine();
	    if (null != line) {    // common case
	      LOG.debug("readEntity: line = {}", line);
	      return line;
	    }
	    
	    if (control.isConnected()) {
	        LOG.info("readEntity: EOF for {}", path);
	        final String name = path.getName();    // final component of path
	        control.emit(name);
	    }

	    return null;
	}

	@Override
	protected void emit(String tuple) {
		output.emit(tuple);
	}

}
