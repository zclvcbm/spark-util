package com.kakfa.offset.test

object othtest {
  def main(args: Array[String]): Unit = {
    val a="/con/ss/das/q/"
    val b=a.split("/")
    var curentpath=""
    b.foreach { file =>
      if(!file.isEmpty()){
        curentpath=curentpath+"/"+file
        println(curentpath)
      }
      
    
    }
    
  }
}