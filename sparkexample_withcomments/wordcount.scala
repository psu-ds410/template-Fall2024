//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object WordCount {  // all code must be inside an object or class
  /* This is an example of a comment you should include to document the purpose of an object
   * 
   * The purpose of this program is to perform a wordcount on the 
   * war and peace dataset in /datasets/wap. A word is defined as 
   * a consecutive sequence of non-blank characters (no space, tab, newline, etc.)
   * so "one!" is an example 4-letter word. For each word, we want to know the
   * total number of times it appears.
   */
  def main(args: Array[String]) = {  // this is the entry point to our code
    val sc = getSC()  // one function to get the sc variable
    val myrdd = getRDD(sc) // on function to get the rdd
    val counts = doWordCount(myrdd) // additional functions to do the computation
    saveit("result", counts)  // save the rdd to HDFS in a directory called "result" (you should change this)
  }
  
  def getSC() = { // get the spark context variable
    val conf = new SparkConf().setAppName("wc")  //change the app name
    val sc = new SparkContext(conf)
    sc
  }

  def getRDD(sc:SparkContext) = { // Document the RDD
     /*
      * This is an RDD in which each entry is a string corresponding to 
      * a line of text.
      */
     sc.textFile("/datasets/wap")
  }

  def getTestRDD(sc: SparkContext) = { // create a small testing rdd
     val mylines = List(" a  bbb \t  bbb cc   ddd",
                        "cc bbb ddd",
                        "ddd")
     sc.parallelize(mylines, 3)

  }

  def doWordCount(input: RDD[String]) = { // the interesting stuff happens here
    // it is a separate function so we can test it out
    // the input is an rdd, so we can swap in the testing or big data rdd
    // Your comments should include the "plan" (description of input and output and how
    // the function works). Afterwards, there should be examples of entries in the
    // different RDDs the function creates. Note that the size of the comments is much
    // larger than the size of the code.
    /*
     * The purpose of this function is to count the number of times each word
     * appears in the input RDD.
     *
     * Input: an RDD in which each entry is a string, corresponding to a line of text
     * Output: an RDD where each entry is a (word, count) tuple. The word is a string
     *    and the count is a Long and contains the number of times the word appears in the input
     *
     * This function works by reading the RDD, splitting the lines by whitespace characters,
     * removing blank words, then converting each occurence of a word into a tuple (word, 1) so that we 
     * can add up all of the 1's in the occurences associated with a word.
     */

    /* An example of input is an RDD with entries
    RDD input:
    " a  bbb \t  bbb cc   ddd"
    "cc bbb ddd"
    "ddd"
     */ 
    val words = input.flatMap(_.split("\\W+")) //split by whitespace
    /* RDD words:
      " "    this is because there is a space before the "a"
      "a"
      "bbb"
      "bbb"
      "cc"
     */
    val noblank = words.filter(x => x.size > 0)
    /* RDD noblank
       "a"
       "bbb"
       "cc"
     */
    val kv = noblank.map(word => (word,1L))
    /* RDD kv:
       ("a", 1)
       ("bbb", 1)
       ("cc",  1)
     */
    val counts = kv.reduceByKey((x,y) => x+y)
    /* RDD counts:
       ("a", 1)
       ("bbb", 3)
       ("cc",  2)
     */
    counts
  }

  def saveit(name: String, counts: org.apache.spark.rdd.RDD[(String, Long)]) = { // change types here as appropriate
    /*
     * Saves an RDD in HDFS in a directory specified by the name variable.
     */
    counts.saveAsTextFile(name)
  }
}
