package cn.ffcs.mss.spark.test
import java.util

import com.google.common.collect.ImmutableList
import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning, RoundRobinBeam}
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup}
import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.PeriodGranularity
import io.druid.query.aggregation.{AggregatorFactory, LongSumAggregatorFactory}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.{DateTime, DateTimeZone, Period}

class SimpleEventBeamFactory extends BeamFactory[SimpleEvent]
{
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[SimpleEvent] = SimpleEventBeamFactory.BeamInstance
}

object SimpleEventBeamFactory
{

  //聚合列
  val dimensions : util.List[String]= ImmutableList.of(//人力编码
    "userName", //目的IP
    "destinationIp", //目的PORT
    "destinationPort", //登录专业
    "loginMajor", //登录系统
    "loginSystem", //源IP
    "sourceIp", //源PORT
    "sourcePort", //登录地
    "loginPlace", //常用登录地
    "usedPlace", //是否异地登录
    "isRemote", //操作
    "operate")


  //指标
  val aggregators : util.List[AggregatorFactory]= ImmutableList.of[AggregatorFactory](//上行流量
    new LongSumAggregatorFactory("inputOctets", "inputOctets"), //下行流量
    new LongSumAggregatorFactory("outputOctets", "outputOctets"), //总流量
    new LongSumAggregatorFactory("octets", "octets"), //连接次数
    new LongSumAggregatorFactory("connCount", "connCount"))

  val BeamInstance: Beam[SimpleEvent] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "localhost:2181",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )

    curator.start()

    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo"

    DruidBeams
      .builder[SimpleEvent]()
      .timestampSpec(new TimestampSpec("timeStamp", "auto", null))
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation.create(indexService,dataSource))
      .rollup(DruidRollup.
        create(dimensions,aggregators,new PeriodGranularity(Period.parse("PT1S"),null,
          DateTimeZone.forID("+08:00"))))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      ).buildBeam()
  }
}

// Add this import to your Spark job to be able to propagate events from any RDD to Druid


// Now given a Spark DStream, you can send events to Druid.
//dstream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory))
