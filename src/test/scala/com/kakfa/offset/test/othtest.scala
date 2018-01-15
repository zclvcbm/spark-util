package com.kakfa.offset.test

object othtest {
  def main(args: Array[String]): Unit = {
  val a="s,s,s,s,a"
  a.split(",").map { x => if(x=="s") "1" else "" }
  .foreach { println }
    
  }
}