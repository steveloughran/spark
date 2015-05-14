package org.apache.hive.service.server

import org.apache.hive.service.server.HiveServer2.ServerOptionsProcessor

class ServerOptionsProcessorWrapper {
  def init(args: Array[String]) = {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")
    optionsProcessor.parse(args)
  }
}