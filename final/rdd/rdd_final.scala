
object RDDFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val answer = doFinal(myrdd) // additional functions to do the computation
        saveit(answer, "rdd_final")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {

    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
         
    }

    def doFinal(input: RDD[String]): RDD[(String, (Int, Int))] = {

    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {

    }

    def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {

    }

    def saveit(myrdd: RDD[(String, (Int, Int))], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

