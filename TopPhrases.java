import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import java.util.stream.Stream;

import java.nio.file.Files;
import java.nio.file.Paths;

public class TopPhrases {
  /**
   * This field keeps the limit of memory used before suspending the reading process 
   * and starting the writing process of current read data
   */
  private static /*final*/ byte MEMORY_LIMIT = 100;
  /**
   * This field keeps the limit of memory used before switching between reading maps 
   * and starting the writing process of current read data
   */
  private static /*final*/ byte READ_LIMIT = 80;
  
  /**
   * The main map used for reading from the input file
   * This map keeps phrases as keys and their associated counters as values
   * Set the initial capacity and load factor high
   */
  private static Map<String, Integer> readerMap = new ConcurrentHashMap<String, Integer>(600000, 1);
  /**
   * The secondary map used for reading from the input file
   * This map is used while the writing process uses the main map
   * This map has the same structure as the main map
   */
  private static Map<String, Integer> bufferMap = new ConcurrentHashMap<String, Integer>(300000, 1);
  /**
   * The solutions set, used to store the top phrases
   */
  private static Set<TopPhrases.Solution> solutionsSet = new TreeSet<TopPhrases.Solution>();
  
  /**
   * Lock object used for locking code in methods for communication between reader and writer threads
   */
  private static Lock lock = new ReentrantLock();
  /**
   * Lock Condition object used for sending signals between reader and writer threads
   */
  private static Condition condition = lock.newCondition();
  
  /**
   * The number of phrases to be searched, defaults to 100000
   */
  private static int top = 100000;
  /**
   * The path to the big input file
   */
  private static String inputPath;
  /**
   * The path to the even temporary file, used to refresh the mapping phrases-counters file at each reading step
   */
  private static String temp0Path;
  /**
   * The path to the odd temporary file, used to refresh the mapping phrases-counters file at each reading step
   */
  private static String temp1Path;
  /**
   * The path to the solutions file
   */
  private static String solutionsPath;
  
  /**
   * Reading flag
   * This flag will be true as long as the reader thread reads from the input file
   */
  private static  boolean reading = true;
  /**
   * Writing flag
   * This flag will be true only when the writer thread is writing read data from map to the output file
   */
  private static  boolean writing = false;
  
  /**
   * The class that contains the definition to a solution (phrase and the associated count)
   * This class implements comparable comparing the solutions by count
   * The compareTo method has inversed logic in order to have the solutions set ordered from higher to lower
   * Two solution objects cannot be equal in this context cause the phrases are unique so can be stored in a Set
   */
  public static class Solution implements Comparable<Solution> {
    private String phrase;
    private int count;
    
    public Solution(String phrase, int count) {
      this.setPhrase(phrase);
      this.setCount(count);
    }
    
    public String getPhrase() {
      return this.phrase;
    }
    public void setPhrase(String phrase) {
      this.phrase = phrase;
    }
    public int getCount() {
      return this.count;
    }
    public void setCount(int count) {
      this.count = count;
    }
    
    @Override
    public int compareTo(Solution s) {
      if (this.getCount() == s.getCount()) {
        return this.getPhrase().compareTo(s.getPhrase());
      } else if (this.getCount() > s.getCount()) {
        return -1;
      } else {
        return 1;
      }
    }
  }
  
  /**
   * The class that contains the definition of the reader thread
   * This thread will read line by line data from input file until the file finishes
   * and put every phrase in the main class maps adding new or updating the counter
   * The thread will wait until current step write finishes only when the used memory will reach MEMORY_LIMIT
   * The thread will send signals to the writer thread every time the used memory will reach READ_LIMIT
   * The thread will switch between readerMap and bufferMap when READ_LIMIT flag is reached
   */
  private static class ReaderThread implements Runnable {
    @Override
    public void run() {
      System.out.println(TopPhrases.getTimeString() + " - Reader thread started.");
      Stream<String> readerChannel = null;
      try {
        //initialize line counter
        int lineCounter = 0;
        //current used map
        Map<String, Integer> usedMap;
        //start reading
        try {
          readerChannel = Files.lines(Paths.get(TopPhrases.inputPath));
          Iterator<String> iterator = readerChannel.iterator();
          while (iterator.hasNext()) {
            String line = iterator.next();
            //time to check if the memory limit was reached for the case when writer thread is working 
            //and in the same time reader thread is putting  too much data in the secondary map
            //cause in this case the reader thread must wait for writer thread to finish
            //also for exceptional cases for example if writer thread just finished
            TopPhrases.checkMemory(true, lineCounter);
            if (TopPhrases.writing) {
              //if writing thread is running store data in the secondary map
              //cause the main map is used for writing
              usedMap = TopPhrases.bufferMap;
            } else {
              //if the writing thread is not running it may be that secondary map has data
              //and it's time to transfer it to the main map
              if (!TopPhrases.bufferMap.isEmpty()) {
                TopPhrases.readerMap.putAll(TopPhrases.bufferMap);
                TopPhrases.bufferMap.clear();
                //good time for garbage collection cause this code is entered only after writing finishes
                System.gc();
              }
              //use the main map when writer thread not running
              usedMap = TopPhrases.readerMap;
              //time to check if the used memory has reached the read limit
              //the reading limit is checked only if not the writer thread is running
              //cause when the reading limit is reached the writer starts and the writer runs once a time
              TopPhrases.checkMemory(false, lineCounter);
              //if check memory started the writing thread store data in the secondary map
              //cause the main map is used for writing
              if (TopPhrases.writing) {
                usedMap = TopPhrases.bufferMap;
              }
            }
            //split the current input file line into phrases
            String[] phrases = line.split("\\|");
            for (int i = 0; i < phrases.length; i++) {
              String phrase = phrases[i].trim();
              //if we have a valid not empty phrase
              if (phrase.length() > 0) {
                //if phrase already in the map update his counter 
                //else just put it with the counter initialised to 1
                usedMap.put(phrase, usedMap.getOrDefault(phrase, 0) + 1);
              }
            }
            //increment line counter
            lineCounter++;
          }
        } catch (Exception ie) {
          throw ie;
        } finally {
          try {
            readerChannel.close();
          } catch (Exception x) {;}
        }
        //at this point all the lines from the input file have been read
        //in the case the writer thread is running wait for him to finish
        //cause some not processed values may still be in the buffer map
        if (TopPhrases.writing) {
          while (TopPhrases.writing) {
            Thread.sleep(1000);
            Thread.yield();
          }
        }
        System.out.println(TopPhrases.getTimeString() + 
                               " - All lines from the input have been read.");
        System.out.println(TopPhrases.getTimeString() + 
                           " - Final step (write last phrases)." + 
                           lineCounter + " read lines." + 
                           TopPhrases.readerMap.size() + " phrases in the reader map.");
        //copy possible remaining values from secondary map to the main map
        if (!TopPhrases.bufferMap.isEmpty()) {
          TopPhrases.readerMap.putAll(TopPhrases.bufferMap);
          TopPhrases.bufferMap.clear();
        }
        //it's time to set the reading flag to false to tell to the writer thread to stop the infinite loop
        TopPhrases.reading = false;
        //give the signal to the writer thread to write the last phrases
        TopPhrases.waitForWriting(false);
      } catch (InterruptedException ie) {
        System.out.println(TopPhrases.getTimeString() + " - Reader thread interrupted.");
        ie.printStackTrace();
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        try {
          readerChannel.close();
        } catch (Exception e) {;}
      }
      System.out.println(TopPhrases.getTimeString() + " - Reader thread finished.");
    }
  }
  /**
   * The class that contains the definition of the writer thread
   * This thread will wait until the main reading map is full take the map
   * and write it to one of the temporary output files
   * Every writing step the thread will write from the previously wrote temporary file
   * to the other one switching between files (reader/writer) every iteration
   * in order to keep the data updated, also will compare to the data in the full main map
   * updating values where present and after previous file is read adding remaining items 
   * from the map in the current output file
   * Every update step while reading from the previous temp file the founded phrases in the map are removed
   * in order to remain only the new phrases (not present in output), 
   * assuring this way unicity of phrases in the output, cause the final goal of this thread is to create 
   * a file containing all the phrases (no duplicates) one by line with the counters fully updated
   * This thread will continue running like a daemon while the reading flag is on
   * waiting at every step for the signal from the reading thread that the main reading map got populated 
   */
  private static class WriterThread implements Runnable {
    @Override
    public void run() {
      System.out.println(TopPhrases.getTimeString() + " - Writer thread started.");
      BufferedWriter writerChannel = null;
      Stream<String> readerChannel = null;
      try {
        //while reading thread is running run like a daemon
        while (TopPhrases.reading) {
          //wait for the signal from the reader thread that reader map is full
          TopPhrases.waitForReading();
          
          System.out.println(TopPhrases.getTimeString() + " - Writer thread start writing.");
          try { 
            //compare the two temporary output files by last modified date
            //the most recent will become input and the other one output switching every step
            boolean evenOdd = (((new File(TopPhrases.temp0Path).lastModified()) > (new File(TopPhrases.temp1Path).lastModified()))?true:false);
            //create reader and writer streams
            readerChannel = Files.lines(Paths.get((evenOdd?TopPhrases.temp0Path:TopPhrases.temp1Path)));
            writerChannel = new BufferedWriter(new FileWriter((evenOdd?TopPhrases.temp1Path:TopPhrases.temp0Path), false));
            Iterator<String> iterator = readerChannel.iterator();
            //start reading line by line from the previous step output temporary file
            while (iterator.hasNext()) {
              String line = iterator.next().trim();
              //every line of temporary file contains a phrase with it's associated counter separated by a pipe |
              //use pipe cause the phrases cannot contain it, counter position 0 , phrase position 1
              String[] phrase = line.split("\\|");
              Integer mapCount = TopPhrases.readerMap.get(phrase[1]);
              //if this phrase is in the reading map update the line to be wrote on output by 
              //summing the counters from map and previous step temp file
              if (mapCount != null) {
                line = (mapCount + Integer.valueOf(phrase[0])) + "|" + phrase[1];
                TopPhrases.readerMap.remove(phrase[1]);
              }
              //write the line (updated or not) to the current step temp file
              writerChannel.write(line);
              writerChannel.newLine();
            }
            //close the input stream cause we don't needde anymore
            readerChannel.close(); readerChannel = null;
            //iterate through the remaining phrases (the new ones) in the main reading map
            //and write every one at the end of the current temp file
            for (Map.Entry<String, Integer> entry : TopPhrases.readerMap.entrySet()) {
              writerChannel.write(entry.getValue() + "|" + entry.getKey());
              writerChannel.newLine();
            }
            //clear the main reader mao to prepare it for a new full by the reader thread
            //the next step for this thread (if reading flag on) 
            //is to wait for the signal from reader thread that the map is full again
            TopPhrases.readerMap.clear();
            writerChannel.close(); writerChannel = null;
            //System.gc();
          } catch (Exception e) {
            throw e;
          } finally {
            try {
              writerChannel.close();
            } catch (Exception x) {;}
            try {
              readerChannel.close();
            } catch (Exception x) {;}
          }
          System.out.println(TopPhrases.getTimeString() + " - Writer thread finished writing.");
        }
      } catch (InterruptedException ie) {
        System.out.println(TopPhrases.getTimeString() + " - Weiter thread interrupted.");
        ie.printStackTrace();
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        try {
          readerChannel.close();
        } catch (Exception e) {;}
        try {
          writerChannel.close();
        } catch (Exception e) {;}
      }
      System.out.println(TopPhrases.getTimeString() + " - Writer thread finished.");
    }
  }
  /**
   * This class contains the definition of the finder thread
   * The finder thread has the role to find, store and write to a file, all the top solutions to our problem,
   * by searching them in the last updated output temp file by the writer thread
   * This file will contain all the phrases with them fully updated counters so 
   * this will be easy job for the finder thread
   * The solutions will be stored in a set wich will be returned by the main caller method searchTopPhrases
   * The solutions will be also wrote in a solutions file
   * This thread will wait for the writer thread to finish in order to have the complete updated temp file
   */
  private static class FinderThread implements Runnable {
    @Override
    public void run() {
      BufferedWriter writerChannel = null;
      Stream<String> readerChannel = null;
      try {
        System.out.println(TopPhrases.getTimeString() + " - Finder thread started.");
        try { 
          //initialize line counter and phrases counter
          //this counters will be displayed in the solutions file
          int checkCounter = 0;
          int lineCounter = 0;
          //check the last modified temporary file to read from and create the associated input stream
          boolean evenOdd = (((new File(TopPhrases.temp0Path).lastModified()) > (new File(TopPhrases.temp1Path).lastModified()))?true:false);
          readerChannel = Files.lines(Paths.get(evenOdd?TopPhrases.temp0Path:TopPhrases.temp1Path));
          Iterator<String> iterator = readerChannel.iterator();
          //start reading
          while (iterator.hasNext()) {
            //take counter and phrase (separated by a pipe) and create Solution object
            String[] phraseCounter = iterator.next().trim().split("\\|");
            TopPhrases.Solution solution = new TopPhrases.Solution(phraseCounter[1], 
                                                                   Integer.valueOf(phraseCounter[0]));
            //update the counters
            checkCounter += solution.getCount();
            lineCounter++;
            //add the solution in the set
            //the Solution class has compareTo method implemented in a way that 
            //in a sorted set the first one will have the highest counter
            if (TopPhrases.solutionsSet.size() < TopPhrases.top) {
              //if we did not add yet TOP phrases just add the soulution to the set
              TopPhrases.solutionsSet.add(solution);
            } else {
              //if we reached the TOP solutions limit add the current solution only if it's a good one
              //wich means that it has the counter bigger than the lower from set(the last entry)
              if (solution.compareTo(((TreeSet<TopPhrases.Solution>)TopPhrases.solutionsSet).last()) < 0) {
                //pol the last and the new
                ((TreeSet<TopPhrases.Solution>)TopPhrases.solutionsSet).pollLast();
                TopPhrases.solutionsSet.add(solution);
              }
            }
          }
          readerChannel.close();
          //start writing the solutions file
          writerChannel = new BufferedWriter(new FileWriter(TopPhrases.deleteAndCreateFile(TopPhrases.solutionsPath), false));
          //write the header with the counters
          writerChannel.write("The input file had " + checkCounter + " total(including duplicates) phrases.");
          writerChannel.newLine();
          writerChannel.write("The input file had " + lineCounter + " unique phrases.");
          writerChannel.newLine();
          writerChannel.write("Solutions:");
          writerChannel.newLine();
          //write a solution per line reading from the solutions set
          for(TopPhrases.Solution solution : TopPhrases.solutionsSet) {
            writerChannel.write("Phrase : " + solution.getPhrase() + " - " + solution.getCount() + " times.");
            writerChannel.newLine();
          }
          writerChannel.close();
          //delete the temporary files cause we don't need it anymore
          TopPhrases.deleteFile(TopPhrases.temp0Path);
          TopPhrases.deleteFile(TopPhrases.temp1Path);
        } catch (Exception e) {
          throw e;
        } finally {
          try {
            writerChannel.close();
          } catch (Exception x) {;}
          try {
            readerChannel.close();
          } catch (Exception x) {;}
        }
      } catch (InterruptedException ie) {
        System.out.println(TopPhrases.getTimeString() + " - Finder thread interrupted.");
        ie.printStackTrace();
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        try {
          readerChannel.close();
        } catch (Exception e) {;}
        try {
          writerChannel.close();
        } catch (Exception e) {;}
      }
      System.out.println(TopPhrases.getTimeString() + " - Finder thread finished.");
    }
  }
  
  /**
   * Method used by the Writer Thread to signal and wait for the Reader Thread
   */
  private static void waitForReading() throws InterruptedException {
    TopPhrases.lock.lock();
    try {
      //not writing anymore
      //this will tell to the reader that he can put data again in the main reader map
      //putting also the data from the buffer map
      TopPhrases.writing = false;
      //give signal in case reader thread has reached the critical memory limit and is waiting
      TopPhrases.condition.signal();
      //wait for a signal from the reader thread 
      while(!TopPhrases.writing)
        TopPhrases.condition.await();
    } finally {
      TopPhrases.lock.unlock();
    }
  }
  /**
   * Method used by the Reader Thread to signal and wait for the Writer Thread
   */
  private static void waitForWriting(boolean wait) throws InterruptedException {
    TopPhrases.lock.lock();
    try {
      //set the writing flag to true
      TopPhrases.writing = true;
      //give signal to the writer thread to start his job
      //cause the main reader map is full 
      TopPhrases.condition.signal();
      //in case the reader thread reached the critical memory limit this method receives
      //wait parameter set to true and the reader thread will wait for a signal from writer
      //thread wich by the way consumes the map(memory)
      if (wait) {
        while(TopPhrases.writing)
          TopPhrases.condition.await();
      }
    } finally {
      TopPhrases.lock.unlock();
    }
  }
  
  /**
   * This method checks memory limit weather reading or critical depending on the critical argument
   * This method will be invoked only by the reader thread
   * @param critical - if true will check for the critical memory limit(default 100MB) 
   * otherwise to the reading memory limit(default 80MB)
   * @param lineCounter - the current read lines from input file used for logging purpose
   */
  private static void checkMemory(boolean critical, int lineCounter) throws InterruptedException {
    //compute used memory from runtime object
    Runtime runtime = Runtime.getRuntime();
    long memory = runtime.totalMemory() - runtime.freeMemory();
    //select the used limit
    byte limit = (critical?TopPhrases.MEMORY_LIMIT:TopPhrases.READ_LIMIT);
    //if limit reached
    if (memory >= (1024 * 1024 * limit)) {
      //good time for garbage collection
      System.gc();
      //print statistics to the standard output
      System.out.println(TopPhrases.getTimeString() + 
                         " - Reading reached " + limit + " MB limit." + (critical?"Reader thread waits.":""));
      if (!critical) {
        System.out.println(TopPhrases.getTimeString() + " - " + 
                           lineCounter + " read lines." + 
                           TopPhrases.readerMap.size() + " phrases in the reader map.");
      }
      //give signal to the writer thread and if critical also wait for signal
      TopPhrases.waitForWriting(critical);
    }
  }
  
  /**
   * This method returns the current hour and minute in a string used for logging purposes
   */
  private static String getTimeString() {
    Calendar now = Calendar.getInstance();
    return now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE);
  }
  /**
   * This method returns a formatted string containing hours, minutes and seconds computed from a milisecs amount
   */
  private static String getExecutionTimeString(long ms) {
    ms = ms / 1000;
    return (ms / 3600) + "h " + ((ms % 3600) / 60) + "m " + ((ms % 3600) % 60) + "s";
  }
  
  /**
   * This method deletes a file by path, waiting for the process to end
   */
  private static void deleteFile(String path) throws IOException, InterruptedException {
    Runtime.getRuntime().exec("cmd /c del " + path.replace('/','\\') + " /f /q").waitFor();
  }
  /**
   * This method deletes a file by path, waiting for the process to end, and create a new blank copy instead
   */
  private static File deleteAndCreateFile(String path) throws IOException, InterruptedException {
    File file = new File(path);
    if (file.exists()) {
      TopPhrases.deleteFile(path);
    } 
    file.createNewFile();
    return file;
  }
  
  /**
   * This static method sets the memory limits of this application
   * @param criticalLimit - the critical memory limit
   * @param readLimit - the reading memory limit
   */
  public static void setMemoryLimits(byte criticalLimit, byte readLimit) {
    TopPhrases.MEMORY_LIMIT = criticalLimit;
    TopPhrases.READ_LIMIT = readLimit;
  }
  
  /**
   * This static method creates a big input file populating it with default phrases containing random natural numbers
   * @param path - the path to the file to be created
   * @param gb - the size of the file in GB
   * @param phrases - the number of unique phrases generated in the file
   */
  public static void createBigInputFile(String path, byte gb, int phrases) throws Exception {
    BufferedWriter writer = null;
    try {
      long time = System.currentTimeMillis();
      System.out.println(TopPhrases.getTimeString() + " - Start creating input file.");
      File file = TopPhrases.deleteAndCreateFile(path);
      writer = new BufferedWriter(new FileWriter(file, false));
      for(int i = 0; i < (900000 * gb); i++) {
        for (int j = 0; j < 49; j++) {
          writer.write("sample phrase " + ((int)(Math.random() * (phrases)) + 99999) + " | ");
        }
        writer.write("sample phrase " + ((int)(Math.random() * (phrases)) + 99999));
        writer.newLine();
      }
      System.out.println(TopPhrases.getTimeString() + " - Input file created.Time : " + 
                         TopPhrases.getExecutionTimeString(System.currentTimeMillis() - time));
    } catch (Exception ie) {
      throw ie;
    } finally {
      try {
        writer.close();
      } catch (Exception e) {;}
    }
  }
  
  /**
   * This static method search for the top phrases in an input file
   * @param path - the path to the input file
   * @param nrOfPhrases - how many top phrases to search in the file
   */
  public static Set<TopPhrases.Solution> searchTopPhrases(String path, int nrOfPhrases) throws Exception {
    long time = System.currentTimeMillis();
    System.out.println(TopPhrases.getTimeString() + " - Top Phrases searcher started. ");
    if ((new File(path)).exists()) {
      try {
        TopPhrases.top = nrOfPhrases;
        
        String directory = path.substring(0, path.lastIndexOf("/") + 1);
        
        TopPhrases.inputPath = path;
        TopPhrases.temp0Path = directory + "top_phrases_temp_0.tmp";
        TopPhrases.temp1Path = directory + "top_phrases_temp_1.tmp";
        TopPhrases.solutionsPath = directory + "top_phrases_solutions.txt";
        
        TopPhrases.deleteAndCreateFile(TopPhrases.temp0Path);
        TopPhrases.deleteAndCreateFile(TopPhrases.temp1Path);
        
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.submit(new TopPhrases.ReaderThread());
        executor.submit(new TopPhrases.WriterThread()).get();
        executor.submit(new TopPhrases.FinderThread());
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.HOURS);
      } catch (Exception ex) {
        ex.printStackTrace();
        throw ex;
      }
    } else {
      throw new Exception("Input file not found !");
    }
    System.out.println(TopPhrases.getTimeString() + 
                       " - Top Phrases searcher finished.Look into the top_phrases_solutions.txt file.Time : " +
                       TopPhrases.getExecutionTimeString(System.currentTimeMillis() - time));
    return TopPhrases.solutionsSet;
  }

  public static void main(String[] args) {
    try {
      //create an input file of IGB with 1 million unique phrases 
      TopPhrases.createBigInputFile("d:/input.txt", (byte) 1, 1000000);
      //search for the top 100 000 phrases in the file created above
      TopPhrases.searchTopPhrases("d:/input.txt", 100000);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}