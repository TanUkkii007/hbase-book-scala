package ch03.client

import java.net.InetAddress

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object FingerprintExample extends App {

  val put = new Put(Bytes.toBytes("testrow"))
  put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-1"), Bytes.toBytes("val-1"))
  put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-2"), Bytes.toBytes("val-2"))
  put.addColumn(Bytes.toBytes("fam-2"), Bytes.toBytes("qual-3"), Bytes.toBytes("val-3"))

  val id = String.format("Hostname: %s, App: %s", InetAddress.getLocalHost.getHostName, System.getProperty("sun.java.command"))
  put.setId(id)

  System.out.println("Put.size: " + put.size())
  System.out.println("Put.id: " + put.getId)
  System.out.println("Put.fingerprint: " + put.getFingerprint)
  System.out.println("Put.toMap: " + put.toMap())
  System.out.println("Put.toJSON: " + put.toJSON())
  System.out.println("Put.toString: " + put.toString())
}
