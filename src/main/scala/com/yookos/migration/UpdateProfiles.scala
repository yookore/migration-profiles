package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
//import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
//import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * Update legacy users basic profiles
 * @author ${user.name}
 */
object UpdateProfiles extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  implicit val formats = DefaultFormats

  // serializes objects from redis into
  // desired types
  //import com.redis.serialization._
  //import Parse.Implicits._

  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  //var count = 0
  var count = new java.util.concurrent.atomic.AtomicInteger(0)
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_profileupdate_index") == null) 0 else cache.get("latest_legacy_profileupdate_index").toInt

  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
      "url" -> Config.dataSourceUrl(mode, Some("mappings")),
      //"dbtable" -> "legacyusers")
      "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers"
    )
  )

  val legacyDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jiveuserprofile")
  )
  
  // For each of the jiveid in mappings, select
  // a corresponding profile in legacy and write
  // to cassandra
  val profiles = sc.cassandraTable[Profile](s"$keyspace", "legacyuserprofiles").cache()
  val totalProfiles = profiles.cache().cassandraCount()

  val df = mappingsDF.select(mappingsDF("userid"), mappingsDF("yookoreid"))

  reduce(df)

  /**
   * Maps legacyuser to Cassandra profiles
   */ 
  private def reduce(df: DataFrame) = {
    df.collect().foreach(row => {
      val yookoreid = row.getString(1)
      //profiles.filter(csp => csp.userid == yookoreid).foreach(println)
      profiles.filter(csp => csp.userid == yookoreid).foreach {
        profile =>
          println("===profile.userid=== " + profile.userid)
          println("===row(1)=== " + yookoreid)
          cachedIndex = cachedIndex + 1
          cache.set("latest_legacy_profileupdate_index", cachedIndex.toString)
          val userid = row.getLong(0)
          println("===jiveuserid=== " + userid)
          upsert(row, profile, userid)
      }
    })
  }

  /**
   * upsert existing user profile based on legacyuser
   * cum profiles mapping.
   */ 
  private def upsert(row: Row, profile: Profile, jiveuserid: Long) = {
    //legacyDF.select(legacyDF("fieldid"), legacyDF("value"), legacyDF("userid")).filter(f"userid = $jiveuserid%d").collect().foreach(println)
    legacyDF.select(legacyDF("fieldid"), legacyDF("value"), legacyDF("userid")).filter(f"userid = $jiveuserid%d").collect().foreach {
      profileRow =>
        val fieldid = profileRow.getLong(0)
        val value = profileRow.getString(1)
        val biography = p(fieldid, value).get("biography")
        val rstatus = p(fieldid, value).get("relationshipstatus")
        val userid = row.getString(1)
        val birthdate = p(fieldid, value).get("birthdate")
        val homecountry = p(fieldid, value).get("country")
        val title = p(fieldid, value).get("title")
        val terms = profile.terms
        val gender = p(fieldid, value).get("gender")
        val location = p(fieldid, value).get("location")
        val homeaddress = p(fieldid, value).get("homeaddress")
        val creationdate = profile.creationdate
        val lastupdated = profile.lastupdated
        val username = profile.username
        val firstname = profile.firstname
        val lastname = profile.lastname
        val imageurl = Some(null)
        val currentcity = Some(null)
        val timezone = Some(null)
        val hometown = Some(null)
        val currentcountry = p(fieldid, value).get("country")

        println("===profile==== " + Profile(biography, creationdate, lastupdated, rstatus,
          username, hometown, firstname, timezone, currentcity, userid,
          lastname, birthdate, homecountry, title, currentcountry,
          terms, gender, imageurl, homeaddress, location))

        sc.parallelize(Seq(Profile(
          biography, creationdate, lastupdated, rstatus, username, 
          hometown, firstname, timezone, currentcity, userid, 
          lastname, birthdate, homecountry, title, currentcountry,
          terms, gender, imageurl, homeaddress, location)))
            .saveToCassandra(s"$keyspace", "legacyuserprofiles", 
        SomeColumns("biography", "creationdate", "lastupdated", 
          "relationshipstatus", "username", "hometown", "firstname",
          "timezone", "currentcity", "userid", "lastname",
          "birthdate", "homecountry", "title", "currentcountry", 
          "terms", "gender", "imageurl", "homeaddress", "location")
        )
        
          println("===Latest profileupdates cachedIndex=== " + cache.get("latest_legacy_profileupdate_index").toInt)
    }
  }

  def p(field: Long, value: String): Map[String, String] = field match {
    case 1 => Map("title" -> value)
    case 5006 => Map("title" -> value)
    case 8 => Map("biography" -> value)
    case 5001 => Map("gender" -> value)
    case 5009 => Map("country" -> value)
    case 5012 => Map("relationshipstatus" -> value)
    case 5002 => Map("birthdate" -> value)
    case 3 => Map("address" -> value)
    case 4 => Map("phonenumber" -> value)
    case 5 => Map("homephonenumber" -> value)
    case 6 => Map("mobile" -> value)
    case 7 => Map("hiredate" -> value)
    case 9 => Map("expertise" -> value)
    case 10 => Map("alternateemail" -> value)
    case 11 => Map("homeaddress" -> value)
    case 12 => Map("location" -> value)
    case 2 => Map("department" -> value)
    case 5015 => Map("company" -> value)
    case 5018 => Map("counter" -> value)
    case 5010 => Map("hobbies_interest" -> value)
    case 5019 => Map("jobtitle" -> value)
    case 5020 => Map("education" -> value)
  }

  mappingsDF.printSchema()
}
