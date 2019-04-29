package c.a.realtime.caseclass

/**
  * @author ???
  *         2019-04-29 18:11
  */
case class User(mid:String,
                uid:String,
                appid:String,
                area:String,
                os:String,
                ch:String,
                logType:String,
                vs:String,
                var logDate:String,
                var logHour:String,
                var logHourMinute:String,
                var ts:Long
               ) {
}
