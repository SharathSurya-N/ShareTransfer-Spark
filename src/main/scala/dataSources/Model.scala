package dataSources
//Schema for the share details table
case class Shares(
    share_name:String,
    share_holder:String,
    share_owned:Int
)
//Schema for the transfer table
case class Transfer(
    share_tranferor:String,
    share_reciever:String,
    percentage_of_share:Int
)