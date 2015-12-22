from __future__ import unicode_literals
from flask import Flask, Response ,redirect, url_for, session, flash, g, render_template
from flask import request,jsonify
from flask.ext.rauth import RauthOAuth1
from tweepy.auth import OAuthHandler
import requests
from requests_oauthlib import OAuth1
from urlparse import parse_qs
import tweepy
import webbrowser
import json
import redis
consumer_key = "dKUQo0IIf1ZYo3EtMNdbPT374"
consumer_secret = "lnRA8izXCEG6mRMePUV88k6XL3SzUs5fe3iOtKbArMFlaw3jio"
REQUEST_TOKEN_URL = "https://api.twitter.com/oauth/request_token"
AUTHORIZE_URL = "https://api.twitter.com/oauth/authorize?oauth_token="
ACCESS_TOKEN_URL = "https://api.twitter.com/oauth/access_token"
OAUTH_TOKEN = ""
OAUTH_TOKEN_SECRET = ""

app = Flask(__name__)
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
session=dict()
r = redis.StrictRedis(host='127.0.0.1', port=8080, db=0)
resource_owner_key=""
resource_owner_secret=""

def event_stream():
	pubsub=r.pubsub()
	pubsub.subscribe('emotionClassPub')
	for message in pubsub.listen():
		yield 'data: %s\n\n' % message['data']
def twitter_stream():
	pubsub = r.pubsub()
	pubsub.subscribe('tweets')
	for message in pubsub.listen():
		yield 'data: %s\n\n' % message['data']
@app.route('/tweets')
def tweets_stream():
	return Response(twitter_stream(),mimetype="text/event-stream")
@app.route('/map')
def map():
	return render_template('maps.html')
@app.route('/dashboard')
def dashboard():
	return render_template('dashboard.html')


@app.route('/login')
def login():
	oauth = OAuth1(consumer_key, client_secret=consumer_secret)
	r = requests.post(url=REQUEST_TOKEN_URL, auth=oauth)
	credentials = parse_qs(r.content)
	resource_owner_key = credentials.get('oauth_token')[0]
	resource_owner_secret = credentials.get('oauth_token_secret')[0]

	session['resource_owner_key']=(resource_owner_key)
	session['resource_owner_secret']=(resource_owner_secret)
	param={'oauth_token':resource_owner_key}
	authenticate_url='https://api.twitter.com/oauth/authenticate?oauth_token='+resource_owner_key
	return redirect(authenticate_url)

	# auth = tweepy.OAuthHandler(resource_owner_key,resource_owner_secret,'http://0.0.0.0:5000/authorized')
	# auth.secure=True
	# authUrl = auth.get_authorization_url()
	# session['request_token']= (auth.request_token)
	# return redirect(authUrl)

@app.route('/authorized')
def authorized():
	#get verifier and oauth token
	oauth_token=request.values.get("oauth_token")
	oauth_verifier=request.values.get("oauth_verifier")
	resource_owner_key=session['resource_owner_key']
	resource_owner_secret=session['resource_owner_secret']
	oauth = OAuth1(consumer_key,
		client_secret=consumer_secret,
		resource_owner_key=resource_owner_key,
		resource_owner_secret=resource_owner_secret,
		verifier=oauth_verifier)
	# param={'oauth_verifier':oauth_verifier}
	r = requests.post(url=ACCESS_TOKEN_URL, auth=oauth)
	credentials = parse_qs(r.content)
	token = credentials.get('oauth_token')[0]
	secret = credentials.get('oauth_token_secret')[0]
	oauth1 = OAuth1(consumer_key,
		client_secret=consumer_secret,
		resource_owner_key=token,
		resource_owner_secret=secret)
	# verifier= request.args['oauth_verifier']
	# auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
	# if session.get('request_token') ==None:
	# 	return 'no token set'
	# token=session['request_token']
	# del session['request_token']
	# auth.request_token = token
	# auth.get_access_token(verifier)
	# api = tweepy.API(auth)
	# flash('you are logged in')
	req=requests.get('https://api.twitter.com/1.1/account/verify_credentials.json',auth=oauth1)
	if r.status_code==200:
		user=json.loads(req.text)
		session["userId"]=user['id']
		session["userName"]=user['name']
		session["userScreenName"]=user['screen_name']
		session["profileImage"]=user['profile_image_url']
		g.userName=user['name']
		g.profileImage=user['profile_image_url']
		return render_template('dashboard.html')
	else:
		return "not logged in"
	# return redirect(url_for('index'))
@app.route('/')
def homePage():
	return render_template('homePage.html')
@app.route('/index')
def index():
	return render_template('index.html') 
@app.route('/analyse')
def showHomePage():	
	return render_template('home.html') 

@app.route('/stream')
def stream():
	return Response(event_stream(),mimetype="text/event-stream")

if __name__=='__main__':
	app.run(threaded=True,debug=True)