import boto.dynamodb
import time
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection
from TwitterSearch import *
from datetime import datetime

conn = boto.dynamodb.connect_to_region(
        'us-west-2',
        aws_access_key_id='insert key id here',
        aws_secret_access_key='insert secret access key here')

Company_Database ='angelhacks2'

init_table = conn.get_table(Company_Database)
TableGenerator = init_table.scan()
Company_Hashtag_list = []
Company_Tables = []
for i in TableGenerator:
	Company_Hashtag_list.append ((i['companyName'], i['hashtag']))

item_data = {
        'Body': 'Twitter.com',
        'SentBy': 'Avus Main Frame',
        'ReceivedTime': str(datetime.now())}

Twitter_table_schema = conn.create_schema(
	hash_key_name='TwitterHandleTweet',
	hash_key_proto_value=str,
	range_key_name='Score',
	range_key_proto_value=int
	)

def Delete_Tables():
	Remove_list = []
	tables = conn.list_tables()
	print (tables)
	if len(tables) > 1:
		for i in range(0, len(tables)):
			print ("Deleting " + str(tables[i]))
			try:
				if tables[i]!=Company_Database:
					x = conn.get_table(tables[i])
					Remove_list.append(x)
				else:
					print ('Error! AngelHacks too OP')
			except boto.exception.DynamoDBResponseError as e:
				pass	
		for i in range(0, len(Remove_list)):
			try:
				conn.delete_table(Remove_list[i])
			except boto.exception.DynamoDBResponseError as e:
				pass
	init_table = conn.get_table(Company_Database)
	TableGenerator = init_table.scan()
	Company_Hashtag_list = []
	for i in TableGenerator:
		Company_Hashtag_list.append ((i['companyName'], i['hashtag']))

def Create_Company_Tables():
	for i in range(0, len(Company_Hashtag_list)):
		x = Company_Hashtag_list[i][0]
		print("Creating " + str(x))
		s = conn.create_table(
			name=x,
			schema=Twitter_table_schema,
			read_units=20,
			write_units=20
			)
		print("Created " + str(x))
		Company_Tables.append(s)

def Update_Tweets():
	Create_Company_Tables()
	time.sleep(20)
	for i in range(0, len(Company_Hashtag_list)):
		data = final_fn(Company_Hashtag_list[i])
		data = data[Company_Hashtag_list[i][0]]
		for k in range (0, len(data)):
			item = Company_Tables[i].new_item(
			        # Our hash key is 'forum'
			        hash_key= data[k][0],
			        # Our range key is 'subject'
			        range_key=data[k][1],
			        # This has the
			        attrs=item_data
			    )
			item.put()
	print("Finished!")

def get_name(x):
	return x[0]

def get_hashtag(x):
	return x[1]

def score(x):
	return x[1]

cycle_index = 0
def cycle_keys(m): #cycles between m different Twitter keys to prevent going over rate limit
	global cycle_index
	if cycle_index == 0:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 1:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 2:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 3:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 4:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 5:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 6:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 7:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 8:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 9:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )
	elif cycle_index == 10:
		cycle_index = (cycle_index + 1) % m
		return TwitterSearch(
			            consumer_key = 'insert own key',
			            consumer_secret = 'insert own secret',
			            access_token = 'insert own access token',
			            access_token_secret = 'insert own secret access token'
			        )						
		
def final_fn(lst):
	output_dict = {}

	class Company:
		def __init__(self, hashtag):
			self.hashtag = hashtag
			self.data_lst = []
			self.user_lst = []
			self.final_data_lst = []

		def update(self, hashtag):
			try:
			    tso = TwitterSearchOrder()
			    tso.set_keywords([str(hashtag)])
			    tso.set_language('en')

			    ts = cycle_keys(11)

			    def my_callback_closure(current_ts_instance):
			    	queries, tweets_seen = current_ts_instance.get_statistics()
			    	if queries > 0 and (queries % 20 == 0):
			    		time.sleep(20)
			    for tweet in ts.search_tweets_iterable(tso, callback=my_callback_closure):
			    	self.data_lst.append(('@' + tweet['user']['screen_name'] + " tweeted: " + str(tweet['text'].encode('utf-8', 'ignore'))[2:], tweet['retweet_count'] + tweet['user']['followers_count']/5))
			except TwitterSearchException as e:
				pass

	name = get_name(lst)
	hashtag = get_hashtag(lst)
	temp = Company(hashtag)
	temp.update(hashtag)
	temp.data_lst.sort(key = score, reverse = True)
	while len(temp.final_data_lst) <= min(10, len(temp.data_lst) - 2):
		x = temp.data_lst.pop(0)
		if x[0][:12] not in temp.user_lst:
			temp.user_lst.append(x[0][:12])
			temp.final_data_lst.append(x)
		else:
			pass
	output_dict[name] = temp.final_data_lst
	return output_dict
def end():
	Delete_Tables()
	time.sleep(40)	
	Update_Tweets()
	time.sleep(60)
	end()
end()