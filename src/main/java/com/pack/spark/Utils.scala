package com.pack.spark

import java.util._
import java.io.File

class Utils {
  
  def delete(pathString: String) {
    var dir = new File(pathString)
    try {
        var deleted = dir.delete();
        if (deleted) {
            println("Directory removed.")
        } else {
            println("Directory could not be removed")
        }
    }
    catch
    {
      case e: Exception => e.printStackTrace
    }

  }
  
}