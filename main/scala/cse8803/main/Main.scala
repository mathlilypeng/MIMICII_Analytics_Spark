package cse8803.main

/**
 * @author Yue Peng <ypeng63@gatech.edu>
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import cse8803.model._
import cse8803.ioutils.CSVUtils

object Main {
  def main(args: Array[String]){
    val sc = createContext

    val (diagnostic, medication) = loadRddRawData(sc)
    val sptensor_idx = createSptensor(diagnostic, medication).reduce((x, y) => x++y)

    CSVUtils.writeSetAsCSV(sptensor_idx, "result/mimic2-tensor-data.tsv")
  }

  def createSptensor(diagnostic: RDD[Diagnostic], medication: RDD[Medication]): RDD[Set[(Int, Int, Int)]] = {
    val sc = diagnostic.sparkContext

    val diagMerged = diagnostic.map(p => ((p.patientID, p.date), p.icd9code))
                               .groupByKey()
                               .map(p =>(p._1, p._2.toSet)).cache()
    val bc_diag_date_map = sc.broadcast(diagMerged.map(_._1)
                                  .groupByKey()
                                  .map(p => (p._1, p._2.toSet))
                                  .collect()
                                  .toMap)



    val medAssigned = medication.map({p =>
           val candidates: Set[Long] = if(bc_diag_date_map.value.contains(p.patientID)) bc_diag_date_map.value(p.patientID)
           else Set()

           val distanceSet = candidates.map(diagDate => (p.date - diagDate, diagDate))
                                          .filter(diff => diff._1 >=0 && diff._1 <7)


           val assigned_date = if(distanceSet.nonEmpty) distanceSet.toList.sortBy(_._1).take(1)(0)
               else (-1.toLong, 0.toLong)


           (p.patientID, p.medicine, assigned_date)
          })
                              .filter(p => p._3._1 >= 0)
                              .map(p => ((p._1, p._3._2), p._2))

    val diag_med_comb_prev = diagMerged.join(medAssigned)
                                  .map({case((patientID, date),(diagSet, meditem)) =>
        val diag_cross_med = diagSet.map(diagitem => (diagitem, meditem))

            ((patientID, date), diag_cross_med)
        }).map(p => (p._1._1, p._2))
          .reduceByKey((x, y) => x.++(y))
          .map(p => p._2.map({case (icd9code, medName) => (p._1, icd9code, medName)}))

    val sptensor_idx = decide_idx(diag_med_comb_prev)
    sptensor_idx
  }

  def decide_idx(pat_diag_med: RDD[Set[(String, String, String)]]): RDD[Set[(Int, Int, Int)]] = {
    val sc = pat_diag_med.sparkContext
    pat_diag_med.cache()

    val pat_diag_med_all = pat_diag_med.map(set => (set.map(ix => ix._1), set.map(ix => ix._2), set.map(ix => ix._3)))
                                       .reduce((x, y) => (x._1.++(y._1), x._2.++(y._2), x._3.++(y._3)))
    val patMap = pat_diag_med_all._1.zipWithIndex.toMap
    val diagMap = pat_diag_med_all._2.zipWithIndex.toMap
    val medMap = pat_diag_med_all._3.zipWithIndex.toMap

    CSVUtils.writeMapAsCSV(patMap, "result/patDict.csv")
    CSVUtils.writeMapAsCSV(diagMap, "result/diagDict.csv")
    CSVUtils.writeMapAsCSV(medMap, "result/medDict.csv")

    val bcMap = sc.broadcast((patMap, diagMap, medMap))

    val pat_diag_med_idx = pat_diag_med.map(set =>
      set.map(tuple => (bcMap.value._1(tuple._1), bcMap.value._2(tuple._2), bcMap.value._3(tuple._3))))

    pat_diag_med_idx
  }


  def loadRddRawData(sc: SparkContext): (RDD[Diagnostic], RDD[Medication]) = {

    val diagnostic = sc.textFile("data/diagnostics.csv")
                       .map(_.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                       .filter(_(0) != "subject_id")
                       .map(p => Diagnostic(p(0), p(1).toLowerCase, p(2).toLong))
    val medication = sc.textFile("data/medication.csv")
                       .map(_.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                       .filter(_(0) != "subject_id")
                       .map(p => Medication(p(0), p(1).toLowerCase, p(2).toLong))

    (diagnostic, medication)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }
  def createContext(appName: String): SparkContext = createContext(appName, "local")
  def createContext: SparkContext = createContext("CSE 8803 Project", "local")
}


