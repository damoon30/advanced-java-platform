package com.advanced.bigdata.framework.application

import com.advanced.bigdata.framework.common.TApplication
import com.advanced.bigdata.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start("local", "wordCount") {
    val controller = new WordCountController()
    controller.dispatch()
  }

}
