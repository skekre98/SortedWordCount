package com.example.Operator;

import java.util.regex.Pattern;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class WordReader extends BaseOperator {
	private static final Pattern nonWordDefault = Pattern.compile("[\\p{Punct}\\s]+");

	private String nonWordStr;    // configurable regex
	private transient Pattern nonWord;      // compiled regex

	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {

    @Override
    public void process(String line) {
      // line; split it into words and emit them
      final String[] words = nonWord.split(line);
      for (String word : words) {
        if (word.isEmpty()) continue;
        output.emit(word);
      }
    }
	};
	
	public String getnonWordStr() {
		return nonWordStr;
	}
	
	public void setnonWordStr(String regex) {
		nonWordStr = regex;
	}
	
	public Pattern getnonWord() {
		return nonWord;
	}

	@Override
	public void setup(OperatorContext context) {
		if (nonWordStr == null) {
			nonWord = nonWordDefault;
		} else {
			nonWord = Pattern.compile(nonWordStr);
		}
	}
	
	
	
}
