package ch05

import org.apache.hadoop.hbase.NamespaceDescriptor

object NamespaceDescriptorExample extends App {
  val builder = NamespaceDescriptor.create("testspace")
  builder.addConfiguration("key1", "value1")
  val desc = builder.build()
  println("Namespace: " + desc)
}
