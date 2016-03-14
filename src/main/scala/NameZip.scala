package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object NameZip {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("namezip")
        val sc = new SparkContext(conf)

        val userA = args(2)
        val userB = args(3)

        val input = sc.textFile(args(0))
        val userAFriends = input.filter(line => line.startsWith(userA + "\t")).flatMap(line => line.split("\t")(1).split(","))
        val userBFriends = input.filter(line => line.startsWith(userB + "\t")).flatMap(line => line.split("\t")(1).split(","))
        val unionOfFriends = userAFriends.union(userBFriends)
        val mutualFriends = unionOfFriends.map(friend => (friend, 1)).reduceByKey(_ + _).filter(friendCount => (friendCount._2 == 2)).map(friend => (friend._1, friend._1))

        val userData = sc.textFile(args(1))
        val usersInfo = userData.map(user => user.split(",")).map(userInfo => (userInfo(0), userInfo(1) + ":" + userInfo(6)))

        val friendsInfo = mutualFriends.join(usersInfo)
        val output = friendsInfo.map(info => (userA + "\t" + userB, info._2._2)).reduceByKey(_ + ", " + _).map(info => (info._1 + "\t[" + info._2 + "]"))
        output.saveAsTextFile(".\\output")
    }
}