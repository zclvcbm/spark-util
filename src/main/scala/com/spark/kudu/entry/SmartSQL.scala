package com.spark.kudu.entry

trait SmartSQL {
  def rt_rtbreport_byslot_channel_plan(statdate:String) = s"""
 upsert into rt_rtbreport_byslot_channel_plan
 SELECT
  statdate ,
  siteid ,
  plan ,
  slot ,
  channel ,
  pubdomain ,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog  where statdate='${statdate}' and channel != '' and pubdomain !='' and slot !='' 
 group by statdate,siteid,slot,plan,channel,pubdomain;"""

  def rt_rtbreport(statdate:String) = s"""
 upsert into rt_rtbreport
 SELECT
  statdate ,
  siteid ,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog  where statdate='${statdate}'
group by statdate,siteid"""

  def rt_rtbreport_bycreative(statdate:String) = s"""
 upsert into rt_rtbreport_bycreative
 SELECT
  statdate ,
  siteid ,
  creative,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog  where statdate='${statdate}' and creative!=''
group by statdate,siteid,creative"""
  def rt_rtbreport_byhour(statdate:String) = s"""
 upsert into rt_rtbreport_byhour
 SELECT
  statdate ,
  siteid ,
  hour,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}'
group by statdate,siteid,hour"""
  def rt_rtbreport_byactivity(statdate:String) = s"""
 upsert into rt_rtbreport_byactivity
 SELECT
  statdate ,
  siteid ,
  activity,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog  where statdate='${statdate}' and activity!=''
group by statdate,siteid,activity"""

  def rt_rtbreport_byplan(statdate:String) = s"""
 upsert into rt_rtbreport_byplan
 SELECT
  statdate ,
  siteid ,
  plan,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}' 
group by statdate,siteid,plan"""

  def rt_rtbreport_byplan_unit(statdate:String) = s"""
 upsert into rt_rtbreport_byplan_unit
 SELECT
  statdate ,
  siteid ,
  plan,
  unit,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}'  and unit!='' 
group by statdate,siteid,plan,unit"""

  def rt_rtbreport_byplan_unit_creative(statdate:String) = s"""
 upsert into rt_rtbreport_byplan_unit_creative
 SELECT
  statdate ,
  siteid ,
  plan,
  unit,
  creative,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}' and unit!=''  and creative!=''
group by statdate,siteid,plan,unit,creative"""

  def rt_rtbreport_byhour_channel_plan(statdate:String) = s"""
 upsert into rt_rtbreport_byhour_channel_plan
 SELECT
  statdate ,
  siteid ,
  plan,
  hour,
  channel,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}' and channel!=''
group by statdate,siteid,plan,hour,channel"""
  def rt_rtbreport_bydomain_channel_plan(statdate:String) = s"""
 upsert into rt_rtbreport_bydomain_channel_plan
 SELECT
  statdate ,
  siteid ,
  plan,
  pubdomain,
  channel,
  sum(pv) as pv,
  sum(click) as click,
  sum(price) as price
FROM smartadslog where statdate='${statdate}' and channel!='' and pubdomain!=''
group by statdate,siteid,plan,pubdomain,channel"""

}