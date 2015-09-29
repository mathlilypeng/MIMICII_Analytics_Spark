package cse8803.model

/**
 * @author Yue Peng <ypeng63@gatech.edu>.
 */
case class Diagnostic(patientID: String, icd9code: String, date: Long)

case class Medication(patientID: String, medicine: String, date: Long)

