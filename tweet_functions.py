import HTMLParser
import re
from nltk.tokenize import RegexpTokenizer
from nltk.stem.snowball import SnowballStemmer
from pandas import DataFrame

stemmer = SnowballStemmer("english")
tokenizer = RegexpTokenizer(r'#?\w+')
h = HTMLParser.HTMLParser()
# compile a regex to find urls
url_regex = re.compile(r"(?P<url>https?://[^\s]+)")

def tweets_to_list(searched_tweets):
    
    formatted_tweets = []

    for t in searched_tweets:
        t_list = [t.id, t.created_at, t.lang, t.in_reply_to_status_id, t.retweet_count, t.favorite_count, t.user.location, t.user.friends_count, t.user.followers_count, t.user.statuses_count]

        if t.text is not None:
            t_list.extend([h.unescape(t.text)])
        else:
            t_list.extend([None])

        if t.user.screen_name is not None:
            t_list.extend([h.unescape(t.user.screen_name)])
        else:
            t_list.extend([None])

        if t.in_reply_to_screen_name is not None:
            t_list.extend([h.unescape(t.in_reply_to_screen_name)])
        else:
            t_list.extend([None])

        if t.coordinates is not None and t.coordinates['type']=='Point' and t.coordinates['coordinates'][0]<>0:
            t_list.extend([t.coordinates['coordinates'][0], t.coordinates['coordinates'][1]])

        else:
            t_list.extend([None,None])

        if hasattr(t, 'retweeted_status'):
            t_list.extend([t.retweeted_status.id, t.retweeted_status.user.screen_name])
        else:
            t_list.extend([None, None])

        formatted_tweets.append(t_list)
        
    column_list = ['tweet_id','created_at', 'language', 'in_reply_to_status_id', 'retweet_count', 'favorite_count', 'user_location', 'user_friends_count', 'user_followers_count', 'user_statuses_count', 'tweet_text', 'user_screen_name', 'in_reply_to_screen_name', 'longitude', 'latitude', 'retweeted_status_id', 'retweeted_screen_name']

    return [formatted_tweets, column_list]
    
def tweets_to_entity_list(searched_tweets):
    entities_list = []
    
    for tweet in searched_tweets:
        if 'user_mentions' in tweet.entities:
            for mention in tweet.entities['user_mentions']:
                entities_list.append([tweet.id, 'mention', mention['screen_name']])

        if 'urls' in tweet.entities:
            for url in tweet.entities['urls']:
                 entities_list.append([tweet.id, 'url', url['expanded_url']])

        if 'media' in tweet.entities:
            for med in tweet.entities['media']:
                 entities_list.append([tweet.id,med['type'],med['media_url']])

        if 'hashtags' in tweet.entities:
            for tag in tweet.entities['hashtags']:
                 entities_list.append([tweet.id,'hashtag',tag['text']])
        
    column_list = ['tweet_id', 'type', 'value']
    
    return entities_list, column_list
    
def remove_urls(content_string):
    # replace any urls with blank
    return url_regex.sub("",content_string)

def pivot_words(group):
    # define tokenizer to break strings into words (allow hashtags, a-Z0-9)
    # todo: prevent number-only words being tokenized

    # for each row, return a row per token, with sequence number

    row = group.irow(0)
    wordlist = tokenizer.tokenize(row[1])
    return DataFrame({'word':wordlist, 'sequence': range(1,len(wordlist)+1), 'tweet_id':row[0]})