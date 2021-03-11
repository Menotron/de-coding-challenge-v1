package ie.sujesh.apache.spark

import java.net.URL

import sys.process._
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.commons.io.FileUtils
import scala.util.Try

object Utils {

  def fileDownloader(url: String, filename: String): String = {
    new URL(url) #> new File(filename) !!
  }

  def renameAndMoveFile(path: String, newName: String): Unit = {
    val tempDir = new File(path)
    val f1 = tempDir.listFiles((dir,name) => name.matches("part.*(csv|parquet|orc|\\d+)"))(0).getAbsolutePath
    val f2 = new File(path).getParentFile.getAbsolutePath + File.separator + newName
    Try({
      Files.move(Paths.get(f1), Paths.get(f2), StandardCopyOption.REPLACE_EXISTING)
      FileUtils.deleteDirectory(new File(f1.substring(0, f1.lastIndexOf(File.separator))))
    }).getOrElse(false)

  }

  def writeToFile(path: String, content: String) = {
    val buf: Array[Byte] = content.getBytes
    Try(Files.write(Paths.get(path), buf)).getOrElse(false)
  }
}
