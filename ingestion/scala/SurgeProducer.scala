// Custom Scala Kafka producer (PDF: "Driver GPS pings and trip requests streamed to Kafka topics via a custom Scala
// producer"). Scala 2.12, compiled and run inside rh-spark by `make scala-producer` (ingestion/scala/run_scala_producer.sh).
//
//   trip_requests     real NYC TLC requests of REPLAY_DAY, from the current New York time of day onwards
//                     (REPLAY_SAMPLE_RATE sample), re-stamped to "now" with their original gaps - same JSON as tlc_replay
//   driver_gps_pings  its own driver fleet (SCALA_FLEET_SIZE drivers, ids after the Python simulator fleet) pinging every
//                     GPS_PING_MIN_SEC..GPS_PING_MAX_SEC seconds; a free driver in the pickup zone takes a request,
//                     is on_trip for the trip duration and becomes available again in the drop-off zone
//
// Kafka settings as for every producer of the project: acks=all + idempotence (topics have min.insync.replicas=2).
//
//   spark-submit --class ridehail.SurgeProducer surge-producer.jar --seconds 300
package ridehail

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object SurgeProducer {
  val NY: ZoneId = ZoneId.of("America/New_York")
  val TOPIC_TRIPS = "trip_requests"
  val TOPIC_GPS = "driver_gps_pings"
  private val ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  case class Request(offsetSec: Double, pickup: Int, dropoff: Int, miles: Double, tripSec: Int, fare: Double)

  class Driver(val id: Int, var zone: Int) {
    var status: String = "available"
    var nextPingMs: Long = 0L
    var freeAtMs: Long = 0L
    var dropoff: Int = 0
  }

  def env(name: String, default: String): String =
    Option(System.getenv(name)).map(_.trim).filter(_.nonEmpty).getOrElse(default)

  def isoUtc(epochMs: Long): String = ISO.format(Instant.ofEpochMilli(epochMs))

  def jsonString(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  def round(v: Double, digits: Int): Double = BigDecimal(v).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toDouble

  /** Same fields and order as ingestion/producers/tlc_replay.py make_event(). */
  def requestJson(eventId: String, eventMs: Long, r: Request, city: String): String =
    s"""{"event_id":${jsonString(eventId)},"event_time":"${isoUtc(eventMs)}","zone_id":${r.pickup},""" +
      s""""dropoff_zone_id":${r.dropoff},"trip_miles":${round(r.miles, 3)},"trip_time_sec":${r.tripSec},""" +
      s""""fare":${round(r.fare, 2)},"city":${jsonString(city)},"source":"scala_producer"}"""

  /** Same fields as ingestion/producers/gps_simulator.py ping_event(). */
  def pingJson(eventId: String, eventMs: Long, d: Driver, lat: Double, lon: Double, zone: Int, city: String): String =
    s"""{"event_id":${jsonString(eventId)},"event_time":"${isoUtc(eventMs)}","driver_id":${d.id},""" +
      s""""lat":${round(lat, 6)},"lon":${round(lon, 6)},"status":"${d.status}","zone_id":$zone,""" +
      s""""source":"driver","city":${jsonString(city)}}"""

  /** Zone grid written by data/prepare_zones.py: cell "i_j" -> zone, origin, cell size. */
  class Grid(path: String) {
    private val root = new ObjectMapper().readTree(new java.io.File(path))
    val cell: Double = root.get("cell_size").asDouble()
    val lat0: Double = root.get("origin_lat").asDouble()
    val lon0: Double = root.get("origin_lon").asDouble()
    val cells: Map[Int, Array[(Int, Int)]] = root.get("cells").fields().asScala.toSeq
      .map { e => val ij = e.getKey.split("_"); (e.getValue.asInt(), (ij(0).toInt, ij(1).toInt)) }
      .groupBy(_._1).map { case (z, v) => z -> v.map(_._2).toArray }
    val zones: Array[Int] = cells.keys.toArray.sorted

    /** Random point inside a cell owned by the zone, so the streaming job maps it back to the same zone. */
    def randomPoint(zone: Int, rng: Random): (Double, Double) = {
      val cs = cells(zone)
      val (i, j) = cs(rng.nextInt(cs.length))
      (lat0 + (i + rng.nextDouble()) * cell, lon0 + (j + rng.nextDouble()) * cell)
    }
  }

  def parseArgs(args: Array[String]): Map[String, String] =
    args.sliding(2, 2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap

  /** Requests of the replay day from `startSec` (seconds after midnight, New York) for `durationSec` seconds.
    * request_datetime is New York wall-clock time. Spark 3.5 cannot read TIMESTAMP(NANOS) Parquet columns, so the
    * session reads them as epoch nanoseconds (spark.sql.legacy.parquet.nanosAsLong) and they are converted here. */
  def loadRequests(spark: SparkSession, file: String, day: String, startSec: Int, durationSec: Int,
                   rate: Double): Array[Request] = {
    val raw = spark.read.parquet(file)
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val from = java.time.LocalDate.parse(day).atStartOfDay().plusSeconds(startSec)
    val to = from.plusSeconds(durationSec)
    val (t, inWindow) = raw.schema("request_datetime").dataType match {
      case LongType =>
        val ns = (d: java.time.LocalDateTime) => d.toEpochSecond(ZoneOffset.UTC) * 1000000000L
        (expr("timestamp_micros(CAST(request_datetime DIV 1000 AS BIGINT))"),
          col("request_datetime") >= ns(from) && col("request_datetime") < ns(to))
      case tt =>
        (col("request_datetime"),
          col("request_datetime") >= lit(fmt.format(from)).cast(tt) && col("request_datetime") < lit(fmt.format(to)).cast(tt))
    }
    val sod = hour(t) * 3600 + minute(t) * 60 + second(t)
    var df = raw.where(inWindow)
      .where(col("PULocationID").between(1, 263) && col("DOLocationID").between(1, 263)
        && col("trip_miles") > 0 && col("base_passenger_fare") > 0)
    if (rate < 1.0) df = df.sample(withReplacement = false, rate, 7L)
    df.select((sod - startSec).cast("double").alias("off"), col("PULocationID").cast("int"),
        col("DOLocationID").cast("int"), col("trip_miles").cast("double"), col("trip_time").cast("int"),
        col("base_passenger_fare").cast("double"))
      .orderBy("off").collect()
      .map(r => Request(r.getDouble(0), r.getInt(1), r.getInt(2), r.getDouble(3), r.getInt(4), r.getDouble(5)))
  }

  def main(args: Array[String]): Unit = {
    val a = parseArgs(args)
    val seconds = a.getOrElse("seconds", "300").toInt
    val bootstrap = env("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092,kafka3:9092")
    val dataDir = env("DATA_DIR", "/app/data")
    val month = env("TLC_MONTH", "2025-01")
    val day = env("REPLAY_DAY", "2025-01-15")
    val rate = env("REPLAY_SAMPLE_RATE", "0.06").toDouble
    val city = env("CITY", "NYC")
    val fleetSize = env("SCALA_FLEET_SIZE", "100").toInt
    val firstId = env("FLEET_SIZE", "700").toInt + 1            // after the Python GPS simulator fleet (1..FLEET_SIZE)
    val pingMin = env("GPS_PING_MIN_SEC", "15").toDouble
    val pingMax = env("GPS_PING_MAX_SEC", "30").toDouble
    val rng = new Random(13)

    // ---- replayed requests: the replay day from the current New York time of day
    val nowNy = ZonedDateTime.now(NY)
    val startSec = math.min(nowNy.toLocalTime.toSecondOfDay, 86400 - seconds)
    val spark = SparkSession.builder.appName("scala-surge-producer")
      .config("spark.sql.session.timeZone", "UTC").config("spark.sql.legacy.parquet.nanosAsLong", "true").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val requests = loadRequests(spark, s"file://$dataDir/raw/fhvhv_tripdata_$month.parquet", day, startSec, seconds, rate)
    spark.stop()
    val grid = new Grid(s"$dataDir/zone_grid.json")
    println(f"[scala-producer] ${requests.length}%d requests of $day from ${startSec / 3600}%02d:${startSec / 60 % 60}%02d " +
      f"(sample $rate%.3f), fleet $fleetSize%d drivers (ids $firstId%d..${firstId + fleetSize - 1}%d), $seconds%d s")

    // ---- own driver fleet, placed in the zones of the replayed requests (busy zones get more drivers)
    val busy = Some(requests.map(_.pickup).filter(grid.cells.contains)).filter(_.nonEmpty).getOrElse(grid.zones)
    val fleet = (0 until fleetSize).map(i => new Driver(firstId + i, busy(rng.nextInt(busy.length)))).toArray
    val idle = mutable.Map[Int, mutable.Set[Driver]]()
    fleet.foreach(d => idle.getOrElseUpdate(d.zone, mutable.Set()) += d)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("client.id", "scala-surge-producer")
    props.put("acks", "all")
    props.put("enable.idempotence", "true")
    props.put("linger.ms", "20")
    props.put("delivery.timeout.ms", "300000")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val acked, failed = new AtomicLong(0)
    val cb = new Callback {
      override def onCompletion(m: RecordMetadata, e: Exception): Unit =
        if (e == null) acked.incrementAndGet() else { failed.incrementAndGet(); System.err.println(s"delivery failed: $e") }
    }
    def send(topic: String, key: String, value: String): Unit = producer.send(new ProducerRecord(topic, key, value), cb)

    @volatile var stop = false
    @volatile var finished = false
    val mainThread = Thread.currentThread()
    sys.addShutdownHook { stop = true; if (!finished) mainThread.join(15000) }   // Ctrl+C: flush before the JVM exits

    val run = System.currentTimeMillis() / 1000
    val t0 = System.currentTimeMillis()
    fleet.foreach(d => d.nextPingMs = t0 + (rng.nextDouble() * pingMax * 1000).toLong)
    var next = 0
    var sentReq, sentPing, taken = 0L
    var lastLog = t0
    while (!stop && System.currentTimeMillis() - t0 < seconds * 1000L) {
      val now = System.currentTimeMillis()
      // requests that are due (original gap from the replay day, real time)
      while (next < requests.length && t0 + (requests(next).offsetSec * 1000).toLong <= now) {
        val r = requests(next)
        send(TOPIC_TRIPS, r.pickup.toString, requestJson(f"sc-${day.replace("-", "")}-$next%d-$run%d", now, r, city))
        sentReq += 1
        idle.get(r.pickup).flatMap(_.headOption).foreach { d =>      // a free driver in the pickup zone takes it
          idle(r.pickup) -= d
          d.status = "on_trip"; d.dropoff = r.dropoff
          d.freeAtMs = now + math.max(r.tripSec, 60) * 1000L
          taken += 1
        }
        next += 1
      }
      // drivers: finish trips, ping
      fleet.foreach { d =>
        if (d.status == "on_trip" && d.freeAtMs <= now) {
          d.status = "available"; d.zone = if (grid.cells.contains(d.dropoff)) d.dropoff else d.zone
          idle.getOrElseUpdate(d.zone, mutable.Set()) += d
        }
        if (d.nextPingMs <= now) {
          val (lat, lon) = grid.randomPoint(d.zone, rng)
          send(TOPIC_GPS, d.zone.toString, pingJson(java.util.UUID.randomUUID().toString, now, d, lat, lon, d.zone, city))
          sentPing += 1
          d.nextPingMs = now + ((pingMin + rng.nextDouble() * (pingMax - pingMin)) * 1000).toLong
        }
      }
      if (now - lastLog >= 30000) {
        println(s"[scala-producer] ${(now - t0) / 1000}s: trip_requests=$sentReq driver_gps_pings=$sentPing " +
          s"taken_by_fleet=$taken acked=${acked.get} failed=${failed.get}")
        lastLog = now
      }
      Thread.sleep(100)
    }
    producer.flush()
    producer.close()
    finished = true
    println(s"[scala-producer] done: trip_requests=$sentReq driver_gps_pings=$sentPing taken_by_fleet=$taken " +
      s"acked=${acked.get} failed=${failed.get}")
    if (failed.get > 0) sys.exit(1)
  }
}
