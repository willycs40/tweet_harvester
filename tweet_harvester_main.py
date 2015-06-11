
print("Importing Libraries")

from sqlalchemy import create_engine
import tweepy as tweepy
import pandas as pd

print("Importing Packages")

import tweet_functions as tf
from credentials import *


print("Connecting to Database")
db_connection_string = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4&use_unicode=True".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_SCHEMA)
db_engine = create_engine(db_connection_string, encoding="utf8")

print("Connecting to Twitter")
auth = tweepy.OAuthHandler(TWITTER_CONSUMER_TOKEN, TWITTER_CONSUMER_SECRET)
auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


print("Acquiring Batch ID")
connection = db_engine.raw_connection()
try:
    cursor = connection.cursor()
    results = cursor.callproc('start_batch')
    results = list(cursor.fetchall())
    cursor.close()
    connection.commit()
finally:
    connection.close()
batch_id = results[0][0]


print("Acquiring search terms")
term_df = pd.read_sql('call get_terms()', db_engine)


pd.options.mode.chained_assignment = None
max_tweets_per_term = 100


print("Acquiring Tweets...")
for index, term_row in term_df.iterrows():
    
    term_id = term_row['term_id']
    term = term_row['term']
    term_max_id = term_row['max_id']
    print("    Term:{}".format(term))
    
    searched_tweets = []
    last_id = -1
    
    while len(searched_tweets) < max_tweets_per_term:
        count = max_tweets_per_term - len(searched_tweets)
        try:
            new_tweets = api.search(q=term, count=count, max_id=str(last_id - 1), since_id=term_max_id)
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # depending on TweepError.code, one may want to retry or wait
            # to keep things simple, we will give up on an error
            break
    
    number_collected = len(searched_tweets)
    print("        {} tweets collected".format(str(number_collected)))
    
    if number_collected > 0:
        # there were tweets... so need to format and upload them to DB
        
        # first quickly get the new max id and save it 
        new_max_id = str(searched_tweets[0].id)    

        # turn the collection of Status objects into a dataframe
        tweets, headers = tf.tweets_to_list(searched_tweets)
        tweets_df = pd.DataFrame(tweets, columns=headers)

        # make a smaller data frame of just the text, then remove any urls from the text:
        tweet_text_df = tweets_df[['tweet_id','tweet_text']]
        tweet_text_df.loc[:,'tweet_text'] = tweet_text_df['tweet_text'].apply(tf.remove_urls)
        # could also remove other stuff here... maybe @name tags?
        
        # apply tokenizer function to get message_id <-> word table, then stem those words
        words_df = tweet_text_df.groupby('tweet_id', group_keys=False).apply(tf.pivot_words)
        words_df.loc[:,'word'] = words_df['word'].apply(tf.stemmer.stem)

        # going back to the Status objects, now make a dataframe of entities
        entities, headers = tf.tweets_to_entity_list(searched_tweets)
        entities_df = pd.DataFrame(entities, columns=headers)
        
        # add the term ID to tweets df 
        tweets_df['term_id'] = term_id
        
        try:
            connection = db_engine.raw_connection()
            cursor = connection.cursor()
            
            try:
                cursor.callproc('truncate_staging')
            except:
                print("Error truncating staging")
            
            try:
                tweets_df.to_sql('stage_tweet', db_engine, if_exists='append', index=False)
                cursor.callproc('merge_stage_tweet', [str(batch_id)])
            except:
                print("Error loading/merging tweets")
            
            try:
                words_df.to_sql('stage_word', db_engine, if_exists='append', index=False)
                cursor.callproc('merge_stage_word')
            except:
                print("Error loading/merging words")
            
            try:
                entities_df.to_sql('stage_entity', db_engine, if_exists='append', index=False)
                cursor.callproc('merge_stage_entity')
            except:
                print("Error loading/merging entities")            
                
            cursor.close()
            connection.commit()
        finally:
            connection.close()
    
    else:
        # no tweets, so ignore the stuff above, and set the max_id to same as before
        new_max_id = term_max_id
    
    # tweets or no, log the term batch before moving on to next term
    connection = db_engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.callproc('log_batch_term', [str(batch_id), str(term_id), new_max_id, str(number_collected)])
        cursor.close()
        connection.commit()
    finally:
        connection.close()

print("Finished")