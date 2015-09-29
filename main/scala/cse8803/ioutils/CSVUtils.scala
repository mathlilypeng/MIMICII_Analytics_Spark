package cse8803.ioutils

import java.io._
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._


/**
 * @author Yue Peng <ypeng63@gatech.edu>.
 */
object CSVUtils {
  def writeSetAsCSV(output: Set[(Int, Int, Int)], outpath: String) = {

    val writer = new PrintWriter(new File(outpath))
    val data = output.toArray

    for(line <- data){
      writer.println(line.productIterator.toArray.mkString("\t"))
    }
    writer.flush()
    writer.close()
  }

  def writeMapAsCSV(output: Map[String, Int], outpath: String) = {

    val data = output.map(p => Array(p._1, p._2.toString))
    val out = new BufferedWriter(new FileWriter(outpath))
    val writer = new CSVWriter(out)
    writer.writeAll(data.toList)
    writer.flush()
    writer.close()
  }
}
