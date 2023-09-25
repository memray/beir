'''
The following is a basic twitter scraper code using tweepy.
We preprocess the text - 1. Remove Emojis 2. Remove urls from the tweet.
We store the output tweets with tweet-id and tweet-text in each line tab seperated.

You will need to have an active Twitter account and provide your consumer key, secret and a callback url.
You can get your keys from here: https://developer.twitter.com/en/portal/projects-and-apps
Twitter by default implements rate limiting of scraping tweets per hour: https://developer.twitter.com/en/docs/twitter-api/rate-limits
Default limits are 300 calls (in every 15 mins).

Install tweepy (pip install tweepy) to run the code below.
python scrape_tweets.py
'''
import orjson as json
# import json
import os

import tweepy
import csv
import tqdm
import re

#### Twitter Account Details
consumer_key = 'is0DfZhwrvQIGXo5a1gD0vYbH' # Your twitter consumer key
consumer_secret = '71IbFRWCv2zHWyFlNRpHimVnkOGoThYGLakHIOemA7KplJvDB4' # Your twitter consumer secret
access_token = '1381690552584900611-Rt061WvZnTgpf54BP020dqtks1nKES'
access_token_secret = 'BGYJMeOmSdZaWmZmISngcdFIFrzTdrrwtpGrt48lu9Mp9'
callback_url = '127.0.0.1:9999' # callback url

#### Input/Output Details
input_file = "/export/home/data/beir/signal-1m/201509-tweet-ids.txt" # Tab seperated file containing twitter tweet-id in each line
output_file = "/export/home/data/beir/signal-1m/201509-tweet-scraped-text.txt" # output file which you wish to save

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def de_emojify(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

def preprocessing(text):
    return re.sub(r"http\S+", "", de_emojify(text).replace("\n", "")).strip()

def update_tweet_dict(tweets, tweet_dict):
    for tweet in tweets:
        try:
            idx = tweet['id_str'].strip()
            tweet_dict[idx] = preprocessing(tweet['text'])
        except:
            continue
    
    return tweet_dict

def write_tweets_to_jsonl(filename, tweets):
    with open(filename, "w") as outfile:
        for tweet in tweets:
            outfile.write(json.dumps(tweet) + '\n')

def write_dict_to_file(filename, dic):
    with open(filename, "w") as outfile:
        outfile.write("\n".join((idx + "\t" + text) for idx, text in dic.items()))

### Main Code starts here
# authorization of consumer key and consumer secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret, callback_url)
# set access to user's access key and access secret
auth.set_access_token(access_token, access_token_secret)

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print('Error! Failed to get request token.')

api = tweepy.API(auth, wait_on_rate_limit=True, parser=tweepy.parsers.JSONParser())

tweet_ids_to_scrape = []
tweet_dicts = []
tweet_texts = {}
seen_tweet_set = set()

# load previous scraped tweets
if os.path.exists(output_file+'.jsonl'):
    print(f'Loading previously scraped tweets from {output_file+".jsonl"}.')
    with open(output_file+'.jsonl', 'r') as jsonl_file:
        for lnum, l in enumerate(jsonl_file):
            if lnum % 100 == 0: print(f'Loading line {lnum}')
            tweet = json.loads(l)
            tweet_dicts.append(tweet)
            seen_tweet_set.add(tweet['id_str'])
            tweet_texts = update_tweet_dict(tweet_dicts, tweet_texts)
    print(f'Loaded {len(tweet_dicts)} tweets, {len(tweet_texts)} texts.')

# read tweets ids, ignore previous scraped ones
tweet_to_scrape_count = 0
reader = csv.reader(open(input_file, encoding="utf-8"), delimiter="\t", quoting=csv.QUOTE_NONE)
for row in reader:
    tid = row[0].strip()
    tweet_to_scrape_count += 1
    if tid in seen_tweet_set: continue
    tweet_ids_to_scrape.append(tid)

generator = chunks(tweet_ids_to_scrape, 100)
batches = int(len(tweet_ids_to_scrape) / 100)
total = batches if len(tweet_ids_to_scrape) % 100 == 0 else batches + 1

print("Retrieving Tweets...")
for idx, tweet_id_chunks in enumerate(generator):
    # api.lookup_statuses returns full Tweet objects for up to 100 Tweets per request, specified by the id parameter.
    new_tweets = api.lookup_statuses(id=tweet_id_chunks, include_entities=True, trim_user=True, map=None)
    tweet_dicts += [t for t in new_tweets if t]
    if idx % 10 == 0:
        print(f"Progress {idx}/{total}, scraped={len(tweet_dicts)}/{tweet_to_scrape_count}")
    if idx >= 300 and idx % 300 == 0: # Rate-limiting every 300 calls (in 15 mins)
        print(f"Processing batches and write to file")
        tweet_texts = update_tweet_dict(tweet_dicts, tweet_texts)
        write_tweets_to_jsonl(output_file +'.jsonl', tweet_dicts)
        write_dict_to_file(output_file, tweet_texts)

print("Preprocessing the last batch...")
tweet_texts = update_tweet_dict(tweet_dicts, tweet_texts)
write_tweets_to_jsonl(output_file + '.jsonl', tweet_dicts)
write_dict_to_file(output_file, tweet_texts)
print("Scraping done!")
print(f'#(tweet ids)={tweet_to_scrape_count}')
print(f'#(tweets)={tweet_dicts}')
print(f'#(tweet texts)={tweet_texts}')
