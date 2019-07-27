import boto3
import json
import tweepy
from datetime import datetime, timedelta

OUTPUT_BUCKET = 'cf-datalake'
OUTPUT_KEY = 'twitter'
START_DATE = datetime.strftime(datetime.now() - timedelta(2), '%Y-%m-%d')
END_DATE = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

ssm = boto3.client('ssm')
s3 = boto3.resource('s3')

def _get_twitter_api():
    """
    Reads parameters from AWS SSM to create a Tweepy API instance

    Returns:
        Tweepy API instance
    """
    parameter_names = [
        "twitter_consumer_key",
        "twitter_consumer_secret",
        "twitter_access_token",
        "twitter_access_token_secret"
    ]
    result = ssm.get_parameters(
        Names=parameter_names,
        WithDecryption=True
    )

    param_lookup = {param['Name']: param['Value'] for param in result['Parameters']}
    auth = tweepy.OAuthHandler(param_lookup["twitter_consumer_key"], param_lookup["twitter_consumer_secret"])
    auth.set_access_token(param_lookup["twitter_access_token"], param_lookup["twitter_access_token_secret"])

    return tweepy.API(auth, wait_on_rate_limit=True)


def get_tweets(term, start_date, end_date):
    """
    Retrieves tweets via twitter API, given a keyword

    Args:
        term: keyword to search tweets for
        start_date: Start date of time window in YYYY-MM-DD format
        end_date: End date of time window in YYYY-MM-DD format

    Returns:
        data: a dictionary of tweets with respective likes, retweets, and timestamps
    """

    api = _get_twitter_api()

    twitter_data = []

    tweets = tweepy.Cursor(
        api.search,
        q=f"{term} -filter:retweets",
        lang="en",
        since=start_date,
        until=end_date,
    ).items()

    for tweet in tweets:
        data = {
            'term': term,
            'tweet': tweet.text,
            'likes': tweet.favorite_count,
            'retweets': tweet.retweet_count,
            'created_at': tweet.created_at
        }

        twitter_data.append(data)

    return twitter_data


def write_to_s3(data, term, bucket, key, date):
    """
    Writes tweets to s3 in json

    Args:
        data: input data to write to s3
        term: keyword in tweet
        bucket: s3 bucket to write to
        key: s3 key to write to
        date: date partition in YYYY-MM-DD format
    Results:
        As a side effect, data is written to s3
    """
    s3_object = s3.Object(bucket, f'{key}/{date}/{term}_tweets.json')
    s3_object.put(Body=json.dumps(data, separators=(',', ':'), default=str))


def tweet_scraper(event, context):
    """
    AWS Lambda handler function to tweet scraper when service executes your code

    Args:
        event: AWS Lambda parameter to pass in event data to the handler.
        context: AWS Lambda uses this parameter to provide runtime information to your handler

    Returns:
         As a side effect, data is written to s3
    """
    data = get_tweets(event['term'], START_DATE, END_DATE)
    write_to_s3(data, event['term'], OUTPUT_BUCKET, OUTPUT_KEY, START_DATE)