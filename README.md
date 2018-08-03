# SortedWordCount
A scalable Apache Apex application that can read in a large text file and output the number of occurrences of each word in the file from greatest to least. This application has been written in Java and built on Eclipse.

## Built With
This application was built through a directed-acyclic graph of operators and used [Maven](https://maven.apache.org/) for dependency management.

## Operators
**Line Reader**: This operator split the file into lines and passed each line onto the next operator.

**Word Reader**: This operator would read in the line and split it into different words.

**Word Count**: This operator would take the words that it was given and increase their associated frequency or create a new frequency if it sees a new word.

**WCPair**: This operator paired words with their correct frequencies.

**File Word Count**: This operator would run until the EOF is reached and output a sorted list of words based on their frequencies.


