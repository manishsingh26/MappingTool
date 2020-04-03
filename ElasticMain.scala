package KPI

import java.io.File
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

object ElasticMain {
  def main(args: Array[String]): Unit = {

    val mappingCSVPath = "C:\\Users\\m4singh\\IdeaProjects\\KPILearningTool\\Mapping\\csvMapping.csv"
    val dataDirectory = "C:\\Users\\m4singh\\IdeaProjects\\KPILearningTool\\KPIData"
    val countryRenaming = Map("zm" -> "zambia", "ug" -> "uganda")
    val technologyRenaming = Map("2g" -> "two", "3g" -> "three", "4g" -> "four")

    val spark = SparkSession.builder().appName("DataFrame").master("local").getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load(mappingCSVPath)

    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile)
        .filter(_.getName.endsWith(".csv"))
        .map(_.getPath).toList
    }

    val csvPathList = getListOfFiles(dataDirectory)
    for (eachPath <- csvPathList) {
      val fileName = FilenameUtils.getName(eachPath)
      val fileNameList = fileName.split("%").toList
      val mappingDetails = fileNameList.head.split("_").toList

      val country = mappingDetails.head.toLowerCase()
      val technology = mappingDetails(1).toLowerCase
      val kpiName = fileNameList(1).stripSuffix(".csv").stripPrefix("_").toLowerCase()

      val esIndex = df.filter(df("country") === countryRenaming(country) && df("technology") === technologyRenaming(technology) && df("kpi") === kpiName)
                    .select("es_index")
                    .first()
                    .get(0)

      val kpiDF = spark.read.format("csv").option("header", "true").load(eachPath)
      val esIndexMap = esIndex + "/kpi"
      kpiDF.saveToEs(esIndexMap)

    }
    spark.close()
  }
}
