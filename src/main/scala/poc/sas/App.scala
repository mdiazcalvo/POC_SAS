package poc.sas

import java.io._
import java.util.Date
import java.util.logging.{Level, Logger}

import com.epam.parso.impl.{CSVDataWriterImpl, SasFileReaderImpl}
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  
  def main(args : Array[String]): Unit = {
    println(s"Start Test")
    BasicConfigurator.configure()
    org.apache.log4j.Logger.getLogger("com.epam.parso.impl").setLevel(org.apache.log4j.Level.FATAL)
    org.apache.log4j.Logger.getLogger("com.ggasoftware.parso").setLevel(org.apache.log4j.Level.FATAL)
    println(s"Parso Library Test")
    testParso()
    println(s"SparkSQL SAS Library test")
    testSpark()

    def testSpark()={
      val conf = new SparkConf().
        setMaster("local[*]").
        setAppName("test").
        set("spark.ui.enabled", "false").
        set("spark.app.id", "poc")
      val sc=new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      time("Datetime test") {
        convertFileSparkLib(sqlContext,"/home/mdiaz/Documents/data/pruebas_libreria_sas/datetime.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/spark-results/datetime.csv")
      }
      time("1000000 lines test") {
        convertFileSparkLib(sqlContext,"/home/mdiaz/Documents/data/pruebas_libreria_sas/random.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/spark-results/random.csv")
      }

      time("Small file test") {
        convertFileSparkLib(sqlContext,"/home/mdiaz/Documents/data/pruebas_libreria_sas/ccd_sch_029_1516_sas_prel.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/spark-results/ccd_sch_029_1516_sas_prel.csv")
      }

    }

    def testParso()={

      time("Datetime test") {
        convertFileParsoLib("/home/mdiaz/Documents/data/pruebas_libreria_sas/datetime.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/parso-results/datetime.csv")
      }
      time("1000000 lines test") {
        convertFileParsoLib("/home/mdiaz/Documents/data/pruebas_libreria_sas/random.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/parso-results/random.csv")
      }

      time("Small file test") {
        convertFileParsoLib("/home/mdiaz/Documents/data/pruebas_libreria_sas/ccd_sch_029_1516_sas_prel.sas7bdat","/home/mdiaz/Documents/data/pruebas_libreria_sas/parso-results/ccd_sch_029_1516_sas_prel.csv")
      }
    }

    def time(functionName: String)(block: => Unit): Unit = {
      val t0 = System.nanoTime()
      block // call-by-name
      val t1 = System.nanoTime()
      println(s"Took ${(t1 - t0)/1000000} milis executing $functionName")
    }

    def convertFileParsoLib(input:String, output:String)={
      val is = new FileInputStream(new File(input))
      val sasFileReader = new SasFileReaderImpl(is)
      val out = new BufferedWriter(new FileWriter(output))
      val csvDataWriter = new CSVDataWriterImpl(out)
      csvDataWriter.writeRowsArray(sasFileReader.getColumns(), sasFileReader.readAll())
    }
    def convertFileSparkLib(sqlContext:SQLContext,input:String, output:String)={
      val df = sqlContext.read.format("com.github.saurfang.sas.spark").load(input)
      df.write.format("com.databricks.spark.csv").save(output)
    }


  }


}
