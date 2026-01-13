import{_ as o,c as e,o as n,a as d}from"./chunks/framework.oRtYI6b-.js";const l=JSON.parse('{"title":"全国职业技能大赛大数据应用开发赛题任务B详解","description":"","frontmatter":{"title":"全国职业技能大赛大数据应用开发赛题任务B详解","createTime":"2024-09-12T00:00:00.000Z","categoryName":"大数据","tags":["scala"],"cover":null,"summary":"这是一篇测试文章","published":true},"headers":[],"relativePath":"posts/2024/09/b.md","filePath":"posts/2024/09/b.md"}'),a={name:"posts/2024/09/b.md"};function i(r,t,u,s,q,_){return n(),e("div",null,[...t[0]||(t[0]=[d(`<h2 id="_1、写在前言" tabindex="-1">1、写在前言 <a class="header-anchor" href="#_1、写在前言" aria-label="Permalink to “1、写在前言”">​</a></h2><p>关于本篇：博主在团队中主要负责数据抽取、数据清洗、数据代码部分的编写，也就是任务书中的任务B模块，因此本片博客内容主要也会集中在任务B题目上 关于代码风格：任务B的内容基本都是和spark打交道，众所周知spark有两种编写风格sql风格、dsl风格。两种风格功能都是能实现的，博主这里无脑推荐dsl风格（比赛里idea用的是社区版没有数据库插件，sql没有补全，但是dsl的函数是有的），比赛题目都比较简单，因此速度上的越快越好。 关于技巧：1.比赛开始就要将常用方法如读mysql写mysql写到工具类里，这样能节省很多时间，也能防止写错 2.比赛能idea本地跑的部分就不要打包到服务器运行，除了一些必须要服务器截图的题目，方便debug 3.题干关键字划出，尤其是子任务一，子任务一错后面全错。 关于内容更新：博主已经是一个大三的专科仔，主要时间在写毕设和专升本，关于题目会一点点更新，另外因为当时主要练的是国赛任务书，一些地方任务书可能我也没做过就不会贴出来了。</p><h2 id="_2、常用代码封装" tabindex="-1">2、常用代码封装 <a class="header-anchor" href="#_2、常用代码封装" aria-label="Permalink to “2、常用代码封装”">​</a></h2><pre><code>//创建spark 配置
package object jhc {
System.setProperty(&quot;HADOOP_USER_NAME&quot;,&quot;root&quot;)
val odsPath = &quot;hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/&quot;
val dwdPath = &quot;hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/&quot;
val dwsPath = &quot;hdfs://bigdata1:9000/user/hive/warehouse/dws_ds_hudi.db/&quot;
val jdbcUrl = &quot;jdbc:mysql://localhost:3306/shtd_store&quot;
private val conf = new SparkConf()
  .set(&quot;spark.driver.host&quot;,&quot;localhost&quot;)
  .set(&quot;spark.serializer&quot;,&quot;org.apache.spark.serializer.KryoSerializer&quot;)
  .setMaster(&quot;local[*]&quot;)
  .setAppName(&quot;app&quot;)

//创建spark对象
val spark = SparkSession.builder()
  .config(conf)
  .enableHiveSupport()
  .getOrCreate()
spark.sparkContext.setLogLevel(&quot;WARN&quot;)

//mysql 连接信息
private val properties = new Properties()
properties.setProperty(&quot;user&quot;,&quot;root&quot;)
properties.setProperty(&quot;password&quot;,&quot;123456&quot;)
properties.setProperty(&quot;driver&quot;,&quot;com.mysql.jdbc.Driver&quot;)

/**
 * 从mysql加载数据
 * @param table
 * @return
 */
def loadToMysql(table:String): DataFrame = spark.read.jdbc(jdbcUrl,table,properties)



/**
 * 保存到mysql
 * @param table
 * @param dataFrame
 */
def saveToMysql(table: String, dataFrame: DataFrame): Unit = dataFrame.write.jdbc(jdbcUrl, table, properties)


/**
 * 保存到hive
 * @param table
 * @param dataFrame
 * @param partitionCol
 * @param saveMode
 */
def saveToHive(
              table:String,
              partition:String,
              dataFrame: DataFrame,
              saveMode: SaveMode = SaveMode.Append
              ): Unit = {
    dataFrame.write
      .format(&quot;Hive&quot;)
      .mode(saveMode)
      .partitionBy(partition.split(&#39;,&#39;):_*)
      .saveAsTable(table)
}

/**
 * 保存到hudi
 * @param outPutPath
 * @param table
 * @param recordkeyField
 * @param precombineField
 * @param partitionFields
 * @param dataFrame
 */
def saveToHudi(outPutPath:String,
               table:String,
               recordkeyField:String,
               precombineField:String,
               partitionFields:String,
               dataFrame: DataFrame): Unit = {
    dataFrame.write
      .format(&quot;hudi&quot;)
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      //表名
      .option(TBL_NAME.key(),table)
      //主键
      .option(RECORDKEY_FIELD.key(),recordkeyField)
      //分区字段
      .option(PRECOMBINE_FIELD.key(),precombineField)
      //分区字段
      .option(PARTITIONPATH_FIELD.key(),partitionFields)
      .mode(SaveMode.Append)
      .save(s&quot;$outPutPath/$table&quot;)
}
</code></pre><p>}</p><h2 id="_3、大数据应用开发赛题第01套" tabindex="-1">3、大数据应用开发赛题第01套 <a class="header-anchor" href="#_3、大数据应用开发赛题第01套" aria-label="Permalink to “3、大数据应用开发赛题第01套”">​</a></h2><h3 id="_3-1子任务一" tabindex="-1">3.1子任务一 <a class="header-anchor" href="#_3-1子任务一" aria-label="Permalink to “3.1子任务一”">​</a></h3><p>题目内容       编写Scala代码，使用Spark将MySQL的shtd_store库中表user_info、sku_info、base_province、base_region、order_info、order_detail的数据增量抽取到Hive的ods库中对应表user_info、sku_info、base_province、base_region、order_info、order_detail中。</p><p>抽取shtd_store库中user_info的增量数据进入Hive的ods库中表user_info。根据ods.user_info表中operate_time或create_time作为增量字段(即MySQL中每条数据取这两个时间中较大的那个时间作为增量字段去和ods里的这两个字段中较大的时间进行比较)，只将新增的数据抽入，字段名称、类型不变，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.user_info命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取shtd_store库中sku_info的增量数据进入Hive的ods库中表sku_info。根据ods.sku_info表中create_time作为增量字段，只将新增的数据抽入，字段名称、类型不变，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.sku_info命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取shtd_store库中base_province的增量数据进入Hive的ods库中表base_province。根据ods.base_province表中id作为增量字段，只将新增的数据抽入，字段名称、类型不变并添加字段create_time取当前时间，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.base_province命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取shtd_store库中base_region的增量数据进入Hive的ods库中表base_region。根据ods.base_region表中id作为增量字段，只将新增的数据抽入，字段名称、类型不变并添加字段create_time取当前时间，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.base_region命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取shtd_store库中order_info的增量数据进入Hive的ods库中表order_info，根据ods.order_info表中operate_time或create_time作为增量字段(即MySQL中每条数据取这两个时间中较大的那个时间作为增量字段去和ods里的这两个字段中较大的时间进行比较)，只将新增的数据抽入，字段名称、类型不变，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.order_info命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取shtd_store库中order_detail的增量数据进入Hive的ods库中表order_detail，根据ods.order_detail表中create_time作为增量字段，只将新增的数据抽入，字段名称、类型不变，同时添加静态分区，分区字段为etl_date，类型为String，且值为当前比赛日的前一天日期（分区字段格式为yyyyMMdd）。使用hive cli执行show partitions ods.order_detail命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下。 代码</p><pre><code>import org.apache.spark.sql.functions._
import spark.implicits._
object Task1 {
def main(args: Array[String]): Unit = {
    byOperateAndCreate(&quot;user_info&quot;)
    byCreate(&quot;sku_info&quot;)
    byId(&quot;base_province&quot;)
    byId(&quot;base_region&quot;)
    byOperateAndCreate(&quot;order_info&quot;)
    byCreate(&quot;order_detail&quot;)
}

def byOperateAndCreate(table:String): Unit = {
    val odsName = s&quot;ods3.$table&quot;

    val max_time = spark.table(odsName)
      .select(greatest(max(&quot;create_time&quot;), max(&quot;operate_time&quot;)))
      .first()
      .get(0)

    val mysqlDF = if (max_time == null){
        loadMysql(table)
    }else {
        loadMysql(table)
          .where($&quot;create_time&quot; &gt; max_time or $&quot;operate_time&quot; &gt; max_time)
    }


    val result = mysqlDF
      .withColumn(&quot;etl_date&quot;, lit(date))

    saveToHive(odsName, &quot;etl_date&quot;, result)
}


def byCreate(table:String): Unit = {
    val odsName = s&quot;ods3.$table&quot;

    val max_time = spark.table(odsName)
      .select(max(&quot;create_time&quot;))
      .first()
      .get(0)

    val mysqlDF = if (max_time == null){
        loadMysql(table)
    }else {
        loadMysql(table)
          .where($&quot;create_time&quot; &gt; max_time)
    }
    val result = mysqlDF
      .withColumn(&quot;etl_date&quot;, lit(date))

    saveToHive(odsName, &quot;etl_date&quot;, result)
}


    def byId(table:String): Unit = {
    val odsName = s&quot;ods3.$table&quot;

    val max_id = spark.table(odsName)
      .select(max(&quot;id&quot;))
      .first()
      .get(0)

    val mysqlDF = if (max_id == null){
        loadMysql(table)
    }else {
        loadMysql(table)
          .where($&quot;id&quot; &gt; max_id)
    }
       val result = mysqlDF
      .withColumn(&quot;etl_date&quot;, lit(date))
      .withColumn(&quot;create_time&quot;,current_timestamp())

    saveToHive(odsName, &quot;etl_date&quot;, result)
}
</code></pre><h3 id="_3-2子任务二" tabindex="-1">3.2子任务二 <a class="header-anchor" href="#_3-2子任务二" aria-label="Permalink to “3.2子任务二”">​</a></h3><p>​ 编写Scala代码，使用Spark将ods库中相应表数据全量抽取到Hive的dwd库中对应表中。表中有涉及到timestamp类型的，均要求按照yyyy-MM-dd HH:mm:ss，不记录毫秒数，若原数据中只有年月日，则在时分秒的位置添加00:00:00，添加之后使其符合yyyy-MM-dd HH:mm:ss。(若dwd库中部分表没有数据，正常抽取即可)题目内容         编写Scala代码，使用Spark将ods库中相应表数据全量抽取到Hive的dwd库中对应表中。表中有涉及到timestamp类型的，均要求按照yyyy-MM-dd HH:mm:ss，不记录毫秒数，若原数据中只有年月日，则在时分秒的位置添加00:00:00，添加之后使其符合yyyy-MM-dd HH:mm:ss。</p><p>抽取ods库中user_info表中昨天的分区（子任务一生成的分区）数据，并结合dim_user_info最新分区现有的数据，根据id合并数据到dwd库中dim_user_info的分区表（合并是指对dwd层数据进行插入或修改，需修改的数据以id为合并字段，根据operate_time排序取最新的一条），分区字段为etl_date且值与ods库的相对应表该值相等，同时若operate_time为空，则用create_time填充，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”。若该条记录第一次进入数仓dwd层则dwd_insert_time、dwd_modify_time均存当前操作时间，并进行数据类型转换。若该数据在进入dwd层时发生了合并修改，则dwd_insert_time时间不变，dwd_modify_time存当前操作时间，其余列存最新的值。使用hive cli执行show partitions dwd.dim_user_info命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取ods库sku_info表中昨天的分区（子任务一生成的分区）数据，并结合dim_sku_info最新分区现有的数据，根据id合并数据到dwd库中dim_sku_info的分区表（合并是指对dwd层数据进行插入或修改，需修改的数据以id为合并字段，根据create_time排序取最新的一条），分区字段为etl_date且值与ods库的相对应表该值相等，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”。若该条数据第一次进入数仓dwd层则dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。若该数据在进入dwd层时发生了合并修改，则dwd_insert_time时间不变，dwd_modify_time存当前操作时间，其余列存最新的值。使用hive cli查询表dim_sku_info的字段id、sku_desc、dwd_insert_user、dwd_modify_time、etl_date，条件为最新分区的数据，id大于等于15且小于等于20，并且按照id升序排序，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取ods库base_province表中昨天的分区（子任务一生成的分区）数据，并结合dim_province最新分区现有的数据，根据id合并数据到dwd库中dim_province的分区表（合并是指对dwd层数据进行插入或修改，需修改的数据以id为合并字段，根据create_time排序取最新的一条），分区字段为etl_date且值与ods库的相对应表该值相等，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”。若该条数据第一次进入数仓dwd层则dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。若该数据在进入dwd层时发生了合并修改，则dwd_insert_time时间不变，dwd_modify_time存当前操作时间，其余列存最新的值。使用hive cli在表dwd.dim_province最新分区中，查询该分区中数据的条数，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 抽取ods库base_region表中昨天的分区（子任务一生成的分区）数据，并结合dim_region最新分区现有的数据，根据id合并数据到dwd库中dim_region的分区表（合并是指对dwd层数据进行插入或修改，需修改的数据以id为合并字段，根据create_time排序取最新的一条），分区字段为etl_date且值与ods库的相对应表该值相等，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”。若该条数据第一次进入数仓dwd层则dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。若该数据在进入dwd层时发生了合并修改，则dwd_insert_time时间不变，dwd_modify_time存当前操作时间，其余列存最新的值。使用hive cli在表dwd.dim_region最新分区中，查询该分区中数据的条数，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 将ods库中order_info表昨天的分区（子任务一生成的分区）数据抽取到dwd库中fact_order_info的动态分区表，分区字段为etl_date，类型为String，取create_time值并将格式转换为yyyyMMdd，同时若operate_time为空，则用create_time填充，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列，其中dwd_insert_user、dwd_modify_user均填写“user1”，dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。使用hive cli执行show partitions dwd.fact_order_info命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下； 将ods库中order_detail表昨天的分区（子任务一中生成的分区）数据抽取到dwd库中fact_order_detail的动态分区表，分区字段为etl_date，类型为String，取create_time值并将格式转换为yyyyMMdd，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列，其中dwd_insert_user、dwd_modify_user均填写“user1”，dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。使用hive cli执行show partitions dwd.fact_order_detail命令，将结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下。 代码</p><pre><code>import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.implicits._
object Task2 {
def main(args: Array[String]): Unit = {
    task1(&quot;ods3.user_info&quot;,&quot;dwd3.dim_user_info&quot;)
    task2(&quot;ods3.sku_info&quot;,&quot;dwd3.dim_sku_info&quot;)
    task2(&quot;ods3.base_province&quot;,&quot;dwd3.dim_province&quot;)
    task2(&quot;ods3.base_region&quot;,&quot;dwd3.dim_region&quot;)
    task3(&quot;ods3.order_info&quot;,&quot;dwd3.fact_order_info&quot;)
    task4(&quot;ods3.order_detail&quot;,&quot;dwd3.fact_order_detail&quot;)
}

def task1(odsTable:String,dwdTable:String): Unit = {
    //抽取dwd最新
    val dwdDF = spark.sql(
        s&quot;&quot;&quot;
           |select * from $dwdTable
           |where etl_date =(select max(etl_date) from $dwdTable)
           |&quot;&quot;&quot;.stripMargin)

    val odsDF = spark.table(s&quot;$odsTable&quot;)
      //抽取ods昨天
      .where($&quot;etl_date&quot; === date)
      //空值填充
      .withColumn(&quot;operate_time&quot;,coalesce($&quot;operate_time&quot;,$&quot;create_time&quot;))
      .withColumn(&quot;dwd_insert_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_insert_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
      .withColumn(&quot;dwd_modify_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_modify_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))

    val window1 = Window.partitionBy(&quot;id&quot;)
    //按照operate_time 取最新
    val window2 = Window.partitionBy(&quot;id&quot;).orderBy($&quot;operate_time&quot;.desc)

    val result = dwdDF.unionByName(odsDF)
      .withColumn(&quot;dwd_insert_time&quot;, min(&quot;dwd_insert_time&quot;).over(window1))
      .withColumn(&quot;dwd_modify_time&quot;, max(&quot;dwd_modify_time&quot;).over(window1))
      .withColumn(&quot;row_num&quot;, row_number().over(window2))
      //按照operate_time 取最新
      .where($&quot;row_num&quot; === 1)
      .drop(&quot;row_num&quot;)
      //分区字段和ods一致
      .withColumn(&quot;etl_date&quot;,lit(date))

    saveToHive(s&quot;$dwdTable&quot;,&quot;etl_date&quot;,result)
}

def task2(odsTable:String,dwdTable:String): Unit = {
    //抽取dwd最新
    val dwdDF = spark.sql(
        s&quot;&quot;&quot;
           |select * from $dwdTable
           |where etl_date =(select max(etl_date) from $dwdTable)
           |&quot;&quot;&quot;.stripMargin)

    val odsDF = spark.table(s&quot;$odsTable&quot;)
      //抽取ods昨天
      .where($&quot;etl_date&quot; === date)
      .withColumn(&quot;dwd_insert_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_insert_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
      .withColumn(&quot;dwd_modify_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_modify_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))

    val window1 = Window.partitionBy(&quot;id&quot;)
    //按照create_time 取最新
    val window2 = Window.partitionBy(&quot;id&quot;).orderBy($&quot;create_time&quot;.desc)

    val result = dwdDF.unionByName(odsDF)
      .withColumn(&quot;dwd_insert_time&quot;, min(&quot;dwd_insert_time&quot;).over(window1))
      .withColumn(&quot;dwd_modify_time&quot;, max(&quot;dwd_modify_time&quot;).over(window1))
      .withColumn(&quot;row_num&quot;, row_number().over(window2))
      //按照create_time 取最新
      .where($&quot;row_num&quot; === 1)
      .drop(&quot;row_num&quot;)
      //分区字段和ods一致
      .withColumn(&quot;etl_date&quot;,lit(date))

    saveToHive(s&quot;$dwdTable&quot;,&quot;etl_date&quot;,result)
}



def task3(odsTable:String,dwdTable:String): Unit = {
    val odsDF = spark.table(s&quot;$odsTable&quot;)
      //抽取ods昨天
      .where($&quot;etl_date&quot; === date)
      //分区字段为etl_date，类型为String，取create_time值并将格式转换为yyyyMMdd
      .withColumn(&quot;etl_date&quot;,date_format($&quot;create_time&quot;,&quot;yyyyMMdd&quot;))
      //空值填充
      .withColumn(&quot;operate_time&quot;,coalesce($&quot;operate_time&quot;,$&quot;create_time&quot;))
      .withColumn(&quot;dwd_insert_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_insert_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
      .withColumn(&quot;dwd_modify_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_modify_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
      saveToHive(s&quot;$dwdTable&quot;,&quot;etl_date&quot;,odsDF)
}


def task4(odsTable:String,dwdTable:String): Unit = {
    val odsDF = spark.table(s&quot;$odsTable&quot;)
      //抽取ods昨天
      .where($&quot;etl_date&quot; === date)
      //分区字段为etl_date，类型为String，取create_time值并将格式转换为yyyyMMdd
      .withColumn(&quot;etl_date&quot;,date_format($&quot;create_time&quot;,&quot;yyyyMMdd&quot;))
      .withColumn(&quot;dwd_insert_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_insert_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
      .withColumn(&quot;dwd_modify_user&quot;, lit(&quot;user1&quot;))
      .withColumn(&quot;dwd_modify_time&quot;, to_timestamp(current_timestamp(), &quot;yyyy-MM-dd HH:mm:ss&quot;))
       saveToHive(s&quot;$dwdTable&quot;,&quot;etl_date&quot;,odsDF)
}   3.3子任务三
编写Scala代码，使用Spark计算相关指标。
</code></pre><p>注：在指标计算中，不考虑订单信息表中order_status字段的值，将所有订单视为有效订单。计算订单金额或订单总金额时只使用final_total_amount字段。需注意dwd所有的维表取最新的分区。</p><p>题目一内容        本任务基于以下2、3、4小题完成，使用Azkaban完成第2、3、4题任务代码的调度。工作流要求，使用shell输出“开始”作为工作流的第一个job（job1），2、3、4题任务为串行任务且它们依赖job1的完成（命名为job2、job3、job4），job2、job3、job4完成之后使用shell输出“结束”作为工作流的最后一个job（endjob），endjob依赖job2、job3、job4，并将最终任务调度完成后的工作流截图，将截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下；</p><p>题目一代码</p><p>type=command command=echo &#39;end job&#39; dependencies=job2,job3,job4 题目二内容         根据dwd层表统计每个省份、每个地区、每个月下单的数量和下单的总金额，存入MySQL数据库shtd_result的provinceeverymonth表中（表结构如下），然后在Linux的MySQL命令行中根据订单总数、订单总金额、省份表主键均为降序排序，查询出前5条，将SQL语句复制粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下，将执行结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下;</p><p>字段</p><p>类型</p><p>中文含义</p><p>备注</p><p>provinceid</p><p>int</p><p>省份表主键</p><p>provincename</p><p>text</p><p>省份名称</p><p>regionid</p><p>int</p><p>地区表主键</p><p>regionname</p><p>text</p><p>地区名称</p><p>totalconsumption</p><p>double</p><p>订单总金额</p><p>当月订单总金额</p><p>totalorder</p><p>int</p><p>订单总数</p><p>当月订单总数</p><p>year</p><p>int</p><p>年</p><p>订单产生的年</p><p>month</p><p>int</p><p>月</p><p>订单产生的月</p><p>题目二代码 select * from shtd_result.provinceeverymonth order by totalorder desc ,totalconsumption desc,provinceid desc limit 5;</p><pre><code>import spark.implicits._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.storage.StorageLevel
</code></pre><p>​<br> object ThreeTask1 { val date = &quot;20240324&quot; def main(args: Array[String]): Unit = { //order_info 事实表所有数据 val fact_order_info = spark.table(&quot;dwd3.fact_order_info&quot;) .select( $&quot;final_total_amount&quot;, year($&quot;operate_time&quot;).as(&quot;year&quot;), month($&quot;operate_time&quot;).as(&quot;month&quot;), $&quot;province_id&quot; )</p><pre><code>    //省份表维表最新分区数据
    val dim_province = spark.table(&quot;dwd3.dim_province&quot;)
      .where($&quot;etl_date&quot; === date)
      .select(
          $&quot;id&quot;.as(&quot;province_id&quot;),
          $&quot;name&quot;.as(&quot;province_name&quot;),
          $&quot;region_id&quot;
      )

    //地区表维表最新分区数据
    val dim_region = spark.table(&quot;dwd3.dim_region&quot;)
      .where($&quot;etl_date&quot; === date)
      .select(
          $&quot;id&quot;.as(&quot;region_id&quot;),
          $&quot;region_name&quot;
      )

    //连接
    val join = fact_order_info
      .join(broadcast(dim_province), &quot;province_id&quot;)
      .join(broadcast(dim_region), &quot;region_id&quot;)
      .persist(StorageLevel.MEMORY_ONLY)

    //每省每个月
    val result = join
      .groupBy(&quot;province_id&quot;, &quot;province_name&quot;, &quot;region_id&quot;, &quot;region_name&quot;, &quot;year&quot;, &quot;month&quot;)
      .agg(
          //总销量
          count(&quot;final_total_amount&quot;).as(&quot;totalorder&quot;),
          //总销售额
          sum(&quot;final_total_amount&quot;).as(&quot;totalconsumption&quot;),
      ).select(
          $&quot;province_id&quot;.as(&quot;provinceid&quot;),
          $&quot;province_name&quot;.as(&quot;provincename&quot;),
          $&quot;region_id&quot;.as(&quot;regionid&quot;),
          $&quot;region_name&quot;.as(&quot;regionname&quot;),
          $&quot;totalconsumption&quot;.cast(DoubleType),
          $&quot;totalorder&quot;.cast(IntegerType),
          $&quot;year&quot;,
          $&quot;month&quot;
      )

    result
      .orderBy($&quot;totalorder&quot;.desc)
      .show(false)

    //保存到mysql
    saveToMysql(&quot;provinceeverymonth&quot;,result)
}
</code></pre><p>} 题目三内容        请根据dwd层表计算出2020年4月每个省份的平均订单金额和所有省份平均订单金额相比较结果（“高/低/相同”）,存入MySQL数据库shtd_result的provinceavgcmp表（表结构如下）中，然后在Linux的MySQL命令行中根据省份表主键、该省平均订单金额均为降序排序，查询出前5条，将SQL语句复制粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下，将执行结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下;</p><p>字段</p><p>类型</p><p>中文含义</p><p>备注</p><p>provinceid</p><p>int</p><p>省份表主键</p><p>provincename</p><p>text</p><p>省份名称</p><p>provinceavgconsumption</p><p>double</p><p>该省平均订单金额</p><p>allprovinceavgconsumption</p><p>double</p><p>所有省平均订单金额</p><p>comparison</p><p>text</p><p>比较结果</p><p>该省平均订单金额和所有省平均订单金额比较结果，值为：高/低/相同</p><p>题目三代码 select * from shtd_result.provinceavgcmp order by provinceid desc,provinceavgconsumption desc limit 5;</p><pre><code>    import spark.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
</code></pre><p>​<br> object ThreeTask2 { def main(args: Array[String]): Unit = { val fact_order_info = spark.table(&quot;dwd3.fact_order_info&quot;) .where(year($&quot;operate_time&quot;) === 2020 and month($&quot;operate_time&quot;) === 4) .select( $&quot;final_total_amount&quot;, $&quot;province_id&quot; ) val dim_province = spark.table(&quot;dwd3.dim_province&quot;) .where($&quot;etl_date&quot; === date) .select( $&quot;id&quot;.as(&quot;province_id&quot;), $&quot;name&quot;.as(&quot;province_name&quot;), $&quot;region_id&quot; )</p><pre><code>    val dim_region = spark.table(&quot;dwd3.dim_region&quot;)
      .where($&quot;etl_date&quot; === &quot;20240324&quot;)
      .select(
          $&quot;id&quot;.as(&quot;region_id&quot;),
          $&quot;region_name&quot;
      )

    val join = fact_order_info
      .join(broadcast(dim_province), &quot;province_id&quot;)
      .join(broadcast(dim_region), &quot;region_id&quot;)

    //每个省平均销售额
    val provinceAvg = join.groupBy(&quot;province_id&quot;, &quot;province_name&quot;,&quot;region_id&quot;)
      .agg(
          avg(&quot;final_total_amount&quot;).as(&quot;provinceavgconsumption&quot;)
      )

    //所有省的平均销售额
    val regionAvg = join
      .agg(
          avg(&quot;final_total_amount&quot;).as(&quot;allprovinceavgconsumption&quot;)
      )

    //用所有省 和 每个省的表做join
    val result = provinceAvg.crossJoin(broadcast(regionAvg))
      .withColumn(&quot;comparison&quot;,
          when($&quot;provinceavgconsumption&quot; &gt; $&quot;allprovinceavgconsumption&quot;, &quot;高&quot;)
            .when($&quot;provinceavgconsumption&quot; === $&quot;allprovinceavgconsumption&quot;, &quot;相同&quot;)
            .when($&quot;provinceavgconsumption&quot; &lt; $&quot;allprovinceavgconsumption&quot;, &quot;低&quot;)
      )
      .select(
          $&quot;province_id&quot;.as(&quot;provinceid&quot;),
          $&quot;province_name&quot;.as(&quot;provincename&quot;),
          $&quot;provinceavgconsumption&quot;.cast(DoubleType),
          $&quot;allprovinceavgconsumption&quot;.cast(DoubleType),
          $&quot;comparison&quot;
      )

    result.show()
    //保存到mysql
    saveToMysql(&quot;provinceavgcmp&quot;,result)
}
</code></pre><p>} 题目四内容        根据dwd层表统计在两天内连续下单并且下单金额保持增长的用户，存入MySQL数据库shtd_result的usercontinueorder表(表结构如下)中，然后在Linux的MySQL命令行中根据订单总数、订单总金额、客户主键均为降序排序，查询出前5条，将SQL语句复制粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下，将执行结果截图粘贴至客户端桌面【Release\\任务B提交结果.docx】中对应的任务序号下；</p><p>字段</p><p>类型</p><p>中文含义</p><p>备注</p><p>userid</p><p>int</p><p>客户主键</p><p>username</p><p>text</p><p>客户名称</p><p>day</p><p>text</p><p>日</p><p>记录下单日的时间，格式为</p><p>yyyyMMdd_yyyyMMdd</p><p>例如： 20220101_20220102</p><p>totalconsumption</p><p>double</p><p>订单总金额</p><p>连续两天的订单总金额</p><p>totalorder</p><p>int</p><p>订单总数</p><p>连续两天的订单总数</p><p>题目四代码 select * from shtd_result.usercontinueorder order by totalorder desc ,totalconsumption desc,userid desc limit 5;</p><pre><code>   import spark.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
</code></pre><p>​<br> object ThreeTask3 { def main(args: Array[String]): Unit = { val fact_order_info = spark.table(&quot;dwd3.fact_order_info&quot;) .select( $&quot;final_total_amount&quot;, $&quot;user_id&quot;, to_date($&quot;operate_time&quot;).as(&quot;order_day&quot;) ) val dwd_user_info = spark.table(&quot;dwd3.dim_user_info&quot;) .where($&quot;etl_date&quot; === date) .select($&quot;id&quot;.as(&quot;user_id&quot;),$&quot;name&quot;.as(&quot;user_name&quot;))</p><pre><code>    val window1 = Window.partitionBy(&quot;user_id&quot;).orderBy(&quot;order_day&quot;)

    val result = fact_order_info
      .groupBy(&quot;user_id&quot;, &quot;order_day&quot;)
      .agg(
          //每人每天下单总额
          sum(&quot;final_total_amount&quot;).as(&quot;total_amount&quot;),
          //每人每天下单总量
          count(&quot;final_total_amount&quot;).as(&quot;total_count&quot;)
      )
      //获取上条数据的日期
      .withColumn(&quot;last_day&quot;, lag(&quot;order_day&quot;, 1).over(window1))
      //获取上条数据的总额
      .withColumn(&quot;last_day_total_amount&quot;, lag(&quot;total_amount&quot;, 1).over(window1))
      //获取上条数据的总量
      .withColumn(&quot;last_day_total_count&quot;, lag(&quot;total_count&quot;, 1).over(window1))
      //过滤出上条数据是当前的上一天，且当前总金额比上一天大的
      .where(datediff($&quot;order_day&quot;, $&quot;last_day&quot;) === 1 and ($&quot;total_amount&quot; - $&quot;last_day_total_amount&quot; &gt; 0))
      //计算这两天的总额
      .withColumn(&quot;totalconsumption&quot;, $&quot;total_amount&quot; + $&quot;last_day_total_amount&quot;)
      //计算这两天的总数
      .withColumn(&quot;totalorder&quot;, $&quot;total_count&quot; + $&quot;last_day_total_count&quot;)
      //连接日期
      .withColumn(&quot;day&quot;, concat_ws(&quot;_&quot;, date_format($&quot;last_day&quot;,&quot;yyyyMMdd&quot;), date_format($&quot;order_day&quot;,&quot;yyyyMMdd&quot;)))
      //从user表中拿name
      .join(dwd_user_info, &quot;user_id&quot;)
      .select(
          $&quot;user_id&quot;.as(&quot;userid&quot;),
          $&quot;user_name&quot;.as(&quot;username&quot;),
          $&quot;day&quot;,
          $&quot;totalconsumption&quot;.cast(DoubleType),
          $&quot;totalorder&quot;.cast(IntegerType)
      )
      result.show()
    //保存到mysql

    saveToMysql(&quot;usercontinueorder&quot;,result)
}
</code></pre>`,108)])])}const m=o(a,[["render",i]]);export{l as __pageData,m as default};
