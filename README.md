# SortedWordCount
A scalable Apache Apex application that can read in a large text file and output the number of occurrences of each word in the file from greatest to least. This application has been written in Java and built on Eclipse.

## Built With
This application was built through a directed-acyclic graph of operators and used [Maven](https://maven.apache.org/) for dependency management.

## Operators
Line Reader: This operator split the file into lines and passed each line onto the next operator.

