# ML-with-Big-Data---Assignment-1
# CSL7110 Assignment 1  
Name: Shubham Verma  
Roll Number: M25DE1007  

---

## Overview
This assignment implements large-scale text processing using Apache Hadoop (MapReduce) and Apache Spark (PySpark) on the Project Gutenberg dataset (D184MB). The objective is to demonstrate distributed data processing, metadata extraction, document similarity analysis, and graph-based author influence modeling.

---

## Technologies Used
- Apache Hadoop (Single Node Cluster on WSL)
- Apache Spark (PySpark)
- Java (MapReduce)
- Python (Spark)
- HDFS

---

## Hadoop – MapReduce Section

### WordCount Implementation
- Implemented custom WordCount in Java.
- Used `String.replaceAll()` to remove punctuation.
- Used `StringTokenizer` for word splitting.
- Executed on sample input and `200.txt` dataset.
- Measured execution time.
- Experimented with input split size parameter to observe performance impact.

### Key Concepts Demonstrated
- Mapper and Reducer logic
- HDFS file operations
- Replication factor understanding
- Performance tuning using split size

---

## Spark Section

### Q10 – Metadata Extraction
- Loaded dataset using `wholeTextFiles()` (one book per row).
- Extracted:
  - Title
  - Release Date
  - Language
  - Encoding
- Performed basic analysis:
  - Most common language
  - Average title length

### Q11 – TF-IDF & Book Similarity
- Cleaned and tokenized text.
- Removed stop words.
- Computed TF-IDF vectors using Spark MLlib.
- Used cosine similarity to compare books.
- Identified similar books based on vector similarity.

### Q12 – Author Influence Network
- Extracted author and release year.
- Constructed influence network using a 5-year time window.
- Computed:
  - In-degree
  - Out-degree
- Identified top authors based on influence.

---

Question 4,5,6,10,11&12 are added in the same git
