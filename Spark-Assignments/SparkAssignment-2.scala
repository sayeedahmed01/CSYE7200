// Databricks notebook source
import requests._
import ujson._

// COMMAND ----------

//Getting tracks from Spotify
val token = "BQDPYXrbRYs6TwlvmlJAxkTAJIinZwasUdLmjM06Cq1XDEzKv8EP13N7mr6P1E4nGgOHYWDS4r5B-QGRcyxhp6M6nz6OFLYK6FTP-DNXZB74L2PiSaYazVBhe5mWFDx6NDLJfQjZvnXvlN1cdAG0tN7EtS96e1pyvJZ4SY4OLGN441PQndQWILjfkWT6KJNG"
val headers = Map("Authorization" -> s"Bearer $token")
val limit = 100
val offsets = List.range(0, 500, limit)
val urlList = offsets.map(offset => s"https://api.spotify.com/v1/playlists/$playlistId/tracks?offset=$offset&limit=$limit")
val responses = urlList.map(url => requests.get(url, headers = headers))


// COMMAND ----------

// Part 1: Fetching top 10 longest songs
val tracks = responses.flatMap(response => ujson.read(response.text)("items").arr)


// COMMAND ----------

// Displaying the top 10 longest songs
println("Top 10 longest songs:")
for (track <- top10Tracks) {
  val name = track("track")("name").str
  val duration = track("track")("duration_ms").num
  println(s"$name, $duration")
}

// COMMAND ----------

//Part 2: Fetching artist details and sorting by follower count
val artistDetails = top10Tracks.flatMap(_("track")("artists").arr)
  .map(artist => {
    val artistId = artist("id").str
    val artistUrl = s"https://api.spotify.com/v1/artists/$artistId"
    val artistResponse = requests.get(artistUrl, headers = headers)
    val followerCount = ujson.read(artistResponse.text)("followers")("total").num
    (artist("name").str, followerCount)
  })
  .distinct
  .sortBy(_._2)
  .reverse

// COMMAND ----------

// Displaying artist details sorted by follower count
println("\nArtist details sorted by follower count:")
for ((artist, followers) <- artistDetails) {
  println(s"$artist : $followers followers")
}
