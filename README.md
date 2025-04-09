CASSANDRA ASSIGNMENT

SHARE TRANSFER

	In this assignment I need to create a spark Cassandra application which will transfer the shares of the user between each by specifying the percentage of share that should be transfer between the sender and the receiver.
	Here the there were 3 number of shares named share 1, share 2, share 3 and different people who initially owned the shares are Jim, Pam, Dwight.
	The details of the shares of the shareholder were described.
 
POINTS TO BE NOTED:

	The percentage of the shares to be transferred should be less than 100 if it exceeds than 100 then it should throw an error.
	The usage of collect (), take () is not allowed in this assignment.
	The share sender in a transaction should not be the share receiver in another transaction.
	The share receiver in a transaction should not be the share sender in another transaction.
 
SCENARIO 1:

	In this scenario the transfer of the share should be sent to new user from an existing user to the percentage given,
	To achieve this first I have received the input from the user like the sender’s name, receiver name, percentage of share that should be sent.
	After, receiving the input then I had checked whether the percentage that should be sent is less than 100 or else it throws an error displaying that the share transferred should be less than 100. Then with the help of the transaction table I check whether the sender is only in the sender or in the receiver column if so then it displays the message saying that the receiver in an transaction cannot be a sender in an another transaction if the condition is false then the transaction is done.
	First with the help of the flat Map I have traversed the data which is obtained from the database and then the details if the sender is alone filtered and his share is subtracted from the percentage that the transfer is going to happen and then a new row is created with the receiver name and then the share that he obtains from the sender is calculated and then it is displayed over the console.
	After displaying the details of the share transfer then the transaction which had happened here is inserted into the transaction table and then the share table is updated with the new data which is obtained after the transaction.

SCENARIO 2:

	In this scenario, I need to share the transfer between an existing shareholder to an existing shareholder to the percentage of share that is given as the input.
After I have got the sender’s name, receiver’s name, percentage of share that is given. Then I have checked whether the percentage is lesser than 100 or else an error is thrown displaying that the percentage should be lesser than 100. Then I have checked whether the sender in this transaction haven’t been in receiver in any other old transaction and the receiver in this transaction haven't been in sender in any other old transaction if so then it displays that the receiver should be in correct places. And then the share details table is checked so that the receiver has a share to perform the transaction since in this scenario the transaction is performed between an existing user.
	Once these conditions are satisfied then the transaction is happened with the help of the flap map function which will iterate through the dataset which is loaded and then the sender’s share is reduced with the percentage of the share that he has to transfer and then the receiver data is calculated by the share he got from the sender and then it is added with the old data by filtering the share details by the receiver’s name.
	Then the transaction table and the share tables are updated with the updated values after the transaction that had happened.

SCENARIO 3:

	In this scenario I need to make a multiple transaction with the old data that is taken from the database.
	To archive this first I have made a prompt that takes the number of transactions that needed to be performed and then I have iterated the number of transaction and got the sender’s name, receiver’s name, percentage of share that should be sent to the receiver. Every time I get a transaction details I have checked in a way that the sender details I in the share table so that he can make the transaction. And then I checked for both condition which states that the sender should be in the sender in the previous transaction and the receiver should be in the receiver in the previous transaction if so then the transaction is carried out or else displaying the appropriate message stating the reason why it failed to do so.
	To get the details of the multiple transaction I have defined a function which will return the Seq [Transfer] which has all the transaction that needed to be performed. And then with the help of the helper function which will run for the number of transactions. And then for each transaction the details like the sender’s name, receiver’s name and percentage of share for the transaction.
First, I have calculated only the share that should be transferred to the receiver and then I have updated his details to the database and then this will continue for the number of transactions that the user had entered. 
	After that the sender’s data is calculated by grouping the transfer and finding the percentage that the sender should send and then it is detected from the share’s owned by him. And after updated the share details will be updated to the database.
 

