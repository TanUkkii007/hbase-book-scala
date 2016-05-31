package ch05

import org.apache.hadoop.hbase.TableName

object TableNameExample extends App {

  printTableName("testtable")
  printTableName("testspace:testtable")
  printTableName(Some("testspace"), "testtable")
  printTableName(Some("testspace"), "te_st-ta.ble")
  printTableName(Some(""), "TestTable-100")
  printTableName(Some("tEsTsPaCe"), "te_st-table")

  printTableName("")

  // VALID_NAMESPACE_REGEX = "(?:[a-zA-Z_0-9]+)"
  // VALID_TABLE_QUALIFIER_REGEX = "(?:[a-zA-Z_0-9][a-zA-Z_0-9-.]*)"
  printTableName(".testtable")
  printTableName(Some("te_st-space"), "te_st-table")
  printTableName(Some("tEsTsPaCe"), "te_st-table@dev")

  def printTableName(namespaceOpt: Option[String], tablename: String): Unit = {
    print("Given Namespace: " + namespaceOpt.fold("null")(identity) + ", Tablename: " + tablename + " -> ")
    try {
      val tableNameValue = namespaceOpt match {
        case Some(namespace) => TableName.valueOf(namespace, tablename)
        case None => TableName.valueOf(tablename)
      }
      println(tableNameValue)
    } catch {
      case e: Exception => println(e.getClass.getSimpleName + ": " + e.getMessage)
    }
  }

  def printTableName(tablename: String): Unit = printTableName(None, tablename)
}
