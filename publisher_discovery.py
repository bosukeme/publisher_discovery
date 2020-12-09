import twint
import nest_asyncio
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
from pymongo import MongoClient
from urllib.parse import urlparse
import os
from os.path import join, dirname
from dotenv import load_dotenv
from time import sleep 
from geopy.geocoders import Nominatim 
from langdetect import detect
import urllib
import cv2
import numpy as np




geolocator = Nominatim(user_agent = "geoapiExercises") 


dotenv_path = join(dirname(__file__), '.env')

load_dotenv(dotenv_path)
MONGO_URL = os.environ.get('MONGO_URL')
client= MongoClient(MONGO_URL, connect=False)
db = client.publisher_discovery


nest_asyncio.apply()


def available_columns():
    return twint.output.panda.Tweets_df.columns


def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_followings(username):

    c = twint.Config()
    c.Username = username
    c.Pandas = True

    twint.run.Following(c)
    list_of_followings = twint.storage.panda.Follow_df

    return list_of_followings['following'][username]


def get_latest_tweets_from_handle(username, num_tweets, date):

    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
    c.Hide_output = True

    twint.run.Search(c)
    
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags', 
               'username', 'name', 'link', 'urls', 'photos', 'video',
               'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except:
        tweet_df = pd.DataFrame()
                
    return tweet_df


def create_search_strings_from_tweet_df(tweet_df):
    search_string_list = []
    for i in range(len(tweet_df)):
        tweet_text = tweet_df.iloc[i]['tweet']
        search_string = " ".join(tweet_text.split()[0:5])
        search_string_list.append(search_string)

    tweet_df['Search String'] = search_string_list
    return tweet_df


def get_tweet_handles_from_search_term(search_term, num_tweets, date):
    """
    This function does a search on twitter and returns the handles of those who
    posted the tweets that matched the search term the top 5 most liked tweets for
    each search term
    
    ** Come back to this later and remove the part that only picks 
    """
    c = twint.Config()
    c.Search = search_term
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
    c.Hide_output = True

    twint.run.Search(c)
    
    try:
        search_tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags', 
               'username', 'name', 'link', 'urls', 'photos', 'video',
               'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])

        tweet_w_url_inds = []
        for i in range(len(search_tweet_df)):
            url = search_tweet_df.iloc[i]['urls']
            if len(url) > 0:
                tweet_w_url_inds.append(i)

        search_tweet_df_final = search_tweet_df.iloc[tweet_w_url_inds]
        search_tweet_df_final = search_tweet_df.sort_values(by=['nlikes'], ascending=True) # sort by likes in ascending
        search_term_handles = list(search_tweet_df_final['username'])
    except:
        search_term_handles = []
    
    return search_term_handles


"""
Step 1 - Get the date for today and 7 days ago
"""
yesterday = datetime.now() - timedelta(1)
last_week = datetime.now() - timedelta(7)
yesterday_date = datetime.strftime(yesterday, '%Y-%m-%d')
last_week_date = datetime.strftime(last_week, '%Y-%m-%d')


"""
Step 2 - Read in the latest publisher details CSV
"""
publisher_handles_df = pd.read_csv('News Aggregator Handles - Initial Focus.csv')
publisher_countries = list(publisher_handles_df['Country'])
publisher_twitter_handles = list(publisher_handles_df['Twitter Handle'])
publisher_dict = dict(zip(publisher_twitter_handles, publisher_countries))

"""
Step 3 - Create a dictionary of publisher handles(values) for each country (key)
"""
# Create dict of publisher handles and their countries
publisher_twitter_handle_list = []
publisher_country_list = []
for publisher in publisher_dict:
    try:
        publisher_name = publisher[1:].split('\r')[0]
        publisher_country = publisher_dict[publisher]
        publisher_twitter_handle_list.append(publisher_name)
        publisher_country_list.append(publisher_country)
    except:
        pass

publisher_twitter_dict = dict(zip(publisher_twitter_handle_list, publisher_country_list))

# Create dict of countries and their publisher handles
unique_countries = list(set(publisher_countries))
country_handle_list_full = []

for country in unique_countries:
    country_handle_list = []
    for publisher in publisher_twitter_dict:
        if publisher_twitter_dict[publisher] == country:
            country_handle_list.append(publisher)
    country_handle_list_full.append(country_handle_list)

country_publisher_dict = dict(zip(unique_countries, country_handle_list_full))

country_publisher_dict['Startup'].append('mashable')


category = 'Uganda'
handles = ['newvisionwire', 'bukeddeonline', 'DailyMonitor', 'RedPepperUG', 'The_EastAfrican','observerug']
country_publisher_dict.update({category:handles})


def create_potential_publication_handles(search_tweet_df):
    """
    Step 5 - Find similar tweets to the publisher tweets based on a search term of the first 5 words in each tweet - for now we only work on the top 5 tweets in each country.
    We also only store the handles of the top 5 most liked tweets for each search term
    """
    search_term_list_full = []

    for i in range(len(search_tweet_df)):
        print('%s of %s' % (i, len(search_tweet_df)))
        search_term = search_tweet_df.iloc[i]['Search String']
        search_term_handles = get_tweet_handles_from_search_term(search_term, 500, yesterday_date)
        search_term_list_full += search_term_handles

    # Create a dictionary of the unique handles and the count of occurences
    related_handles_dict = Counter(search_term_list_full)
    potential_publication_handles = list(related_handles_dict.keys())
    print(len(potential_publication_handles))
    
    return potential_publication_handles


def potential_publication_handles_save_to_mongodb(potential_publication_handles):

    # Load in the instagram_user collection from MongoDB
    potential_publication_handles_collection = db.potential_publication_handles_collection # similarly if 'testCollection' did not already exist, Mongo would create it
    
    cur = potential_publication_handles_collection.find() ##check the number before adding
    print('We had %s potential_publication_handles entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    handle_list = list(potential_publication_handles_collection.find({},{ "_id": 0, "handles": 1})) 
    handle_list = list((val for dic in handle_list for val in dic.values()))

    
    #loop throup the handles, and add only new enteries
    new_entries_handles = []
    for handle in potential_publication_handles:
        if handle not in handle_list:
            new_entries_handles.append(handle)
            potential_publication_handles_collection.insert_one({"handles": handle})
            
  
    cur = potential_publication_handles_collection.find() ##check the number after adding
    print('We have %s potential_publication_handles entries at the end' % cur.count())
    
    return new_entries_handles


def calculate_user_tweet_metrics(tweet_df):
    """
    This takes the tweet df and gets metrics like the number of valid urls, then the percentage of the dominant url
    """
    # Get the number of tweets found over the period, and average over the week
    num_handle_tweets = len(tweet_df)
    avg_posts = round(len(tweet_df)/7,2)

    # Get the number of tweets that had a valid url
    url_list = list(tweet_df['urls'])
    url_list2 = [url for url in url_list if len(url) > 0]
    num_valid_urls = len(url_list2)

    post_w_link_perc = int(round(num_valid_urls/num_handle_tweets,2)*100)
    
    if post_w_link_perc > 0:

        domain_name_list = []
        for urls in url_list2:
            for url in urls:
                domain = urlparse(url).netloc
                domain_name_list.append(domain) 
        domain_count_dict = Counter(domain_name_list)

        most_common = domain_count_dict.most_common(1)
        primary_domain = most_common[0][0]
        primary_domain_count = most_common[0][1]

        primary_domain_url_perc = int(round(primary_domain_count/num_valid_urls,2)*100)
        primary_domain_tweet_perc = int(round(primary_domain_count/num_handle_tweets,2)*100)
    
    else:
        primary_domain = 'NA'
        primary_domain_url_perc = 0
        primary_domain_tweet_perc = 0

    return post_w_link_perc, primary_domain_url_perc, primary_domain_tweet_perc, primary_domain

def similar(a, b):
    """
    Gets the similarity between any two strings
    """
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()


def url_to_image(url, temp_article_image_path):
    """
    This has been updated compared to the previous one to be able to 
    extract more difficult image links via initialising a browser object.
    """
    opener = urllib.request.URLopener()
    opener.addheader('User-Agent', 'whatever')
    try:
        filename, headers = opener.retrieve(url,temp_article_image_path)
    except:
        urllib.request.urlretrieve(url,temp_article_image_path)
    image = cv2.imread(temp_article_image_path)
    image = cv2.cvtColor(np.uint8(image), cv2.COLOR_BGR2RGB)
    return image


def determine_brand_colours(brand_logo_url):
    """
    This function takes in a url for a brand logo image, and then returns the primarly and secondary
    in hexadecimal based on the colours in the logo image
    """
    # Read in the logo image
    temp_article_image_path = '%s/temp_article_image.jpg' % os.getcwd()
    logo_image = url_to_image(brand_logo_url, temp_article_image_path)
    # Get frequency of all colours found in the image
    colours, count = np.unique(logo_image.reshape(-1,logo_image.shape[-1]), axis=0, return_counts=True)
    # Sort the colours and get the most common non-white colour as the primary colour
    sorted_indices = sorted(range(len(count)), key=lambda k: count[k], reverse=True)
    sorted_indices2 = sorted_indices[0:10]
    sorted_count = [count[i] for i in sorted_indices2]
    sorted_colours = [tuple(colours[i]) for i in sorted_indices2]
    sorted_colours = [colour for colour in sorted_colours if colour != (255, 255, 255)]
    primary_colour = sorted_colours[0]
    ## Determine the secondary colour based on how close the primary colour is to being white
    ## For now the secondary colour is either black or white
    white = (255,255,255)
    black = (0,0,0)
    white_diff = np.subtract(white, primary_colour)
    if sum(white_diff) > 150:
        secondary_colour = white
    else:
        secondary_colour = black
#     print(primary_colour)
    primary_colour_hex = '#%02x%02x%02x' % primary_colour
#     print(secondary_colour)
    secondary_colour_hex = '#%02x%02x%02x' % secondary_colour
    
    return primary_colour_hex, secondary_colour_hex


def process_handles(new_entries_handles):
    """
    Step 6 - Loop through all the potential publication handles and checks for:
    - The handle posting at least 3 times a day
    - More than 50% of the tweets being posted containing a link
    """
    reject_handle_list = []
    processed_handles_list = []

    user_id_list = []
    user_handle_list = []
    user_name_list = []
    user_bio_list = []
    user_profile_image_list = []
    user_url_list = []
    user_join_date_list = []
    user_location_list = []
    user_following_list = []
    user_followers_list = []
    user_verified_list = []
    user_avg_daily_posts = []
    user_post_link_perc = []
    user_primary_domain = []
    user_potential_score = []
    primary_colors = []
    secondary_colors = []

    num_to_process = 50
    for i in range(num_to_process): 
        try:
            twitter_handle = new_entries_handles[i]
            processed_handles_list.append(twitter_handle)
            print('%s of %s' % (i+1, num_to_process))
            try:
                num_tweets = 1000
                user_tweet_df = get_latest_tweets_from_handle(twitter_handle, num_tweets, last_week_date)
                num_user_tweets = len(user_tweet_df)
                print('Twitter handle: %s' % twitter_handle)
                print('Num tweets in last week: %s' % num_user_tweets)
                avg_posts = round(len(user_tweet_df)/7,2)
                print('Avg daily tweets in last week: %s' % avg_posts)
                post_w_link_perc, primary_domain_url_perc, primary_domain_tweet_perc, primary_domain = calculate_user_tweet_metrics(user_tweet_df)
                print('Perc of posts with a link: %s' % post_w_link_perc)
                print('Primary domain: %s' % primary_domain)
                print('Perc of posts with primary domain (compared to number of posts with link): %s' % primary_domain_url_perc)
                print('Perc of posts with primary domain (compared to number of tweets): %s' % primary_domain_tweet_perc)
                print()

                if avg_posts > 1 and post_w_link_perc > 50:

                    try:
                        c = twint.Config()
                        c.Username = twitter_handle
                        c.Store_object = True
                        c.User_full = False
                        c.Pandas =True
                        c.Hide_output = True

                        twint.run.Lookup(c)
                        user_df = twint.storage.panda.User_df.drop_duplicates(subset=['id'])

                        try:
                            user_id = list(user_df['id'])[0]
                        except:
                            user_id = 'NA'

                        try:
                            user_name = list(user_df['name'])[0]
                        except:
                            user_name = 'NA'

                        try:
                            user_bio = list(user_df['bio'])[0]
                        except:
                            user_bio = 'NA'

                        try:
                            user_profile_image = list(user_df['avatar'])[0]
                            primary_color , secondary_color = determine_brand_colours(user_profile_image)
                            
                        except:
                            user_profile_image = 'NA'

                        try:
                            user_url = list(user_df['url'])[0]
                        except:
                            user_url = 'NA'

                        try:
                            user_join_date = list(user_df['join_date'])[0]
                        except:
                            user_join_date = 'NA'

                        try:
                            user_location = list(user_df['location'])[0]
                        except:
                            user_location = 'NA'

                        try:
                            user_following = list(user_df['following'])[0]
                        except:
                            user_following = 'NA'

                        try:
                            user_followers = list(user_df['followers'])[0]
                        except:
                            user_followers = 'NA'

                        try:
                            user_verified = list(user_df['verified'])[0]
                        except:
                            user_verified = 'NA'


                        ## Clean up the primary domain by removing stuff like .com and www.
                        primary_domain_cleaned = primary_domain.replace('www.','')
                        primary_domain_cleaned = primary_domain_cleaned.replace('.com','')
                        primary_domain_cleaned = primary_domain_cleaned.replace('.',' ')
                        primary_domain_cleaned = primary_domain_cleaned.replace('-',' ')

                        # Calculate similarity based on twitter name
                        print('Comparing handle, username and primary domain')
                        print(twitter_handle)
                        print(user_name)
                        print(primary_domain)

                        ## Calculate the string matching score between the twitter name and the primary domain
                        string_match = similar(user_name, primary_domain_cleaned)
                        user_domain_comp_score = string_match*100
                        print('Username-Domain comparison score: %s' % user_domain_comp_score)


                        # Calculate the potential score
                        try:
                            potential_score = int(primary_domain_tweet_perc*user_domain_comp_score)
                        except:
                            potential_score = 0

                        if 'news' in user_bio.lower():
                            potential_score = potential_score*10

                            potential_score = int(potential_score/1000)
                            print('Potential publisher score: %s' % potential_score)
                            print()

                            ## Save details of handle to mongo
                            user_id_list.append(user_id)
                            user_handle_list.append(twitter_handle)
                            user_name_list.append(user_name)
                            user_bio_list.append(user_bio)
                            user_profile_image_list.append(user_profile_image)
                            user_url_list.append(user_url)
                            user_join_date_list.append(user_join_date) 
                            user_location_list.append(user_location)
                            user_following_list.append(user_following)
                            user_followers_list.append(user_followers)
                            user_verified_list.append(user_verified)
                            user_avg_daily_posts.append(avg_posts)
                            user_post_link_perc.append(post_w_link_perc)
                            user_primary_domain.append(primary_domain)
                            user_potential_score.append(potential_score)
                            primary_colors.append(primary_color)
                            secondary_colors.append(secondary_color)

                    except Exception as e:
                        reject_handle_list.append(twitter_handle)
                        print(e)
                        pass

            except Exception as e:
                print(e)
                pass
        except:
            pass
        
    return user_id_list, user_handle_list, user_name_list, user_bio_list,\
    user_profile_image_list, user_url_list, user_join_date_list, user_location_list,\
    user_following_list, user_followers_list, user_verified_list, user_avg_daily_posts, \
    user_post_link_perc, user_primary_domain, user_potential_score, reject_handle_list, processed_handles_list, \
    primary_colors, secondary_colors



def create_potential_publisher_df(user_id_list, user_handle_list, user_name_list, user_bio_list,\
                                user_profile_image_list, user_url_list, user_join_date_list, user_location_list,\
                                user_following_list, user_followers_list, user_verified_list, user_avg_daily_posts, \
                                user_post_link_perc, user_primary_domain, user_potential_score, primary_colors, secondary_colors):
    
    potential_publisher_df = pd.DataFrame()
    potential_publisher_df['user_id'] = user_id_list
    potential_publisher_df['user_handle'] = user_handle_list
    potential_publisher_df['user_name'] = user_name_list
    potential_publisher_df['user_bio'] = user_bio_list
    potential_publisher_df['profile_image_url'] = user_profile_image_list
    potential_publisher_df['url'] = user_url_list
    potential_publisher_df['join_date'] = user_join_date_list
    potential_publisher_df['location'] = user_location_list
    potential_publisher_df['following'] = user_following_list
    potential_publisher_df['follower'] = user_followers_list
    potential_publisher_df['verified'] = user_verified_list
    potential_publisher_df['avg_daily_post'] = user_avg_daily_posts
    potential_publisher_df['link_post_perc'] = user_post_link_perc
    potential_publisher_df['primary_domain'] = user_primary_domain
    potential_publisher_df['potential_score'] = user_potential_score
    potential_publisher_df['primary_color'] = primary_colors
    potential_publisher_df['secondary_color'] = secondary_colors
    
    
    
    return potential_publisher_df


def remove_rejected_handles(processed_handles_list, reject_handle_list):
    for a in reject_handle_list:
        if a in processed_handles_list:
            processed_handles_list.remove(a)
    return processed_handles_list


def processed_handles_list_save_to_mongodb(processed_handles_list):

    # Load in the instagram_user collection from MongoDB
    processed_handles_collection = db.processed_handles_collection 
    
    cur = processed_handles_collection.find() ##check the number before adding
    print('We had %s processed_handles entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    handle_list = list(processed_handles_collection.find({},{ "_id": 0, "processed_handles": 1})) 
    handle_list = list((val for dic in handle_list for val in dic.values()))

    
    #loop throup the handles, and add only new enteries
    for handle in processed_handles_list:
        if handle not in handle_list:
            processed_handles_collection.insert_one({"processed_handles": handle})
            
  
    cur = processed_handles_collection.find() ##check the number after adding
    print('We have %s processed_handles entries at the end' % cur.count())
    


def rejected_handles_list_save_to_mongodb(rejected_handles_list):

    # Load in the instagram_user collection from MongoDB
    rejected_handles_collection = db.rejected_handles_collection # similarly if 'testCollection' did not already exist, Mongo would create it
    
    cur = rejected_handles_collection.find() ##check the number before adding
    print('We had %s rejected_handles entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    handle_list = list(rejected_handles_collection.find({},{ "_id": 0, "rejected_handles": 1})) 
    handle_list = list((val for dic in handle_list for val in dic.values()))

    
    #loop throup the handles, and add only new enteries
    for handle in rejected_handles_list:
        if handle not in handle_list:
            rejected_handles_collection.insert_one({"rejected_handles": handle})
            
  
    cur = rejected_handles_collection.find() ##check the number after adding
    print('We have %s rejected_handles entries at the end' % cur.count())
    

def calculate_user_account_age(publisher_join_date):
    from datetime import date

    today = datetime.now() 
    today = datetime.strftime(today, '%Y-%m-%d')
    
    today_year = int(today[0:4])
    today_month = int(today[5:7])
    today_day = int(today[8:10])


    join_year = int(publisher_join_date[0:4])
    join_month = int(publisher_join_date[5:7])
    join_day = int(publisher_join_date[8:10])

    join_date = date(join_year, join_month, join_day)
    today_date = date(today_year, today_month, today_day)
    delta = today_date - join_date
    
    return delta



def sort_account_age(potential_publisher_df):
    account_age_list = []
    for i in range(len(potential_publisher_df)):
        publisher_join_date = potential_publisher_df.iloc[i]['join_date']
        publisher_account_age = calculate_user_account_age(publisher_join_date)
        account_age_list.append(publisher_account_age.days)

    potential_publisher_df['account_age'] = account_age_list
    potential_publisher_df = potential_publisher_df.sort_values(by=['account_age'], ascending=True) 
    
    return potential_publisher_df


def save_publisher_df(potential_publisher_df):
    potential_publisher_df_collection = db.potential_publisher_df_collection
    
    cur = potential_publisher_df_collection.find() ##check the number before adding
    print('We had %s potential_publisher entries at the start' % cur.count())
    
    ##search for the entities in the processed colection and store it as a list
    potential_publisher_list = list(potential_publisher_df_collection.find({},{ "_id": 0, "user_handle": 1})) 
    potential_publisher_list = list((val for dic in potential_publisher_list for val in dic.values()))
    
    

    for user_id, user_handle, user_name, user_bio, \
        profile_image_url, url, join_date, location, \
        following, follower, verified, avg_daily_post, \
        link_post_perc, primary_domain, potential_score,\
        account_age, country, date_time, publication_country, lang_bio, primary_color, secondary_color  in  potential_publisher_df[["user_id", 'user_handle', 'user_name', 'user_bio', 
                                                'profile_image_url', 'url', 'join_date', 'location', 
                                                'following', 'follower', 'verified', 'avg_daily_post', 
                                                'link_post_perc', 'primary_domain', 'potential_score', 
                                                'account_age','country', 'date_time', 'publication_country', 'lang_bio', 'primary_color', 'secondary_color']].itertuples(index=False): 
        
        if user_handle not in potential_publisher_list:
            potential_publisher_df_collection.insert_one({"user_id":user_id, 'user_handle':user_handle, 'user_name':user_name,
                                                          'user_bio':user_bio, 'profile_image_url':profile_image_url, 'url':url,
                                                          'join_date':join_date, 'location':location, 'following':following, 
                                                          'follower':follower, 'verified':verified, 'avg_daily_post':avg_daily_post, 
                                                          'link_post_perc':link_post_perc, 'primary_domain':primary_domain,
                                                          'potential_score':potential_score, 'account_age':account_age, 'country':country, 
                                                          'date_time':date_time, 'publication_country':publication_country, 'lang_bio':lang_bio,
                                                         'primary_color':primary_color, 'secondary_color':secondary_color})
            
    cur = potential_publisher_df_collection.find() ##check the number after adding
    print('We have %s potential_publisher entries at the end' % cur.count())
    


def get_country_from_location(location):
    """
    This function takes in 
    """
    if location != 'false':
        country = geolocator.geocode(location) 
        if country is not None:
            country = str(country).split(',')[-1].strip()
        else:
            country = 'NA'
    else:
        country = 'NA'
    return country




def process_all_functions(unique_countries, country_publisher_dict):
    for country in unique_countries:
        print(country)
        print("---------")
        publisher_handle_list = country_publisher_dict[country]
        for twitter_handle in publisher_handle_list: 
            print(twitter_handle)
            try:
                num_tweets = 50
                tweet_df = get_latest_tweets_from_handle(twitter_handle, num_tweets, last_week_date)
                tweet_df = create_search_strings_from_tweet_df(tweet_df)
                tweet_df = tweet_df.reset_index(drop= True)
                tweet_df = tweet_df.sort_values(by=['nlikes'], ascending=False) # sort by likes
                print(len(tweet_df))
                search_tweet_df = tweet_df.iloc[0:25]
                potential_publication_handles = create_potential_publication_handles(search_tweet_df)
                
                new_entries_handles = potential_publication_handles_save_to_mongodb(potential_publication_handles)
                
                user_id_list, user_handle_list, user_name_list, user_bio_list,\
                user_profile_image_list, user_url_list, user_join_date_list, user_location_list,\
                user_following_list, user_followers_list, user_verified_list, user_avg_daily_posts, \
                user_post_link_perc, user_primary_domain, user_potential_score, reject_handle_list, \
                processed_handles_list, primary_colors, secondary_colors = process_handles(new_entries_handles)
                
                
                
                potential_publisher_df = create_potential_publisher_df( user_id_list, user_handle_list, user_name_list, user_bio_list,\
                                                                        user_profile_image_list, user_url_list, user_join_date_list, user_location_list,\
                                                                        user_following_list, user_followers_list, user_verified_list, user_avg_daily_posts, \
                                                                        user_post_link_perc, user_primary_domain, user_potential_score, primary_colors, secondary_colors)
                
                potential_publisher_df['country'] = [country for a in range(len(potential_publisher_df))]
                potential_publisher_df['date_time'] = [datetime.now() for a in range(len(potential_publisher_df))]
                potential_publisher_df['publication_country'] =  [get_country_from_location(a) for a in potential_publisher_df['location']]
                potential_publisher_df['lang_bio'] =  [detect(a) for a in potential_publisher_df['user_bio']]
                

                processed_handles_list = remove_rejected_handles(processed_handles_list, reject_handle_list)
                processed_handles_list_save_to_mongodb(processed_handles_list)
                rejected_handles_list_save_to_mongodb(reject_handle_list)
                
                potential_publisher_df = sort_account_age(potential_publisher_df)
                print(potential_publisher_df)
                
                save_publisher_df(potential_publisher_df)
                
                
                
                print(potential_publisher_df)
                sleep(300)
            except Exception as e:
                print(e)
                pass

        sleep(600)
