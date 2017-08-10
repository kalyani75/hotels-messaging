import os
import redis

import threading
import time

import json
import sys

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
if 'VCAP_SERVICES' in os.environ: 
  vcap_services = json.loads(os.environ['VCAP_SERVICES'])

  uri = '' 
  urimq = ''

  for key, value in vcap_services.iteritems():   # iter on both keys and values
	  if key.find('mysql') > 0 or key.find('cleardb') >= 0:
	    mysql_info = vcap_services[key][0]
		
	    cred = mysql_info['credentials']
	    uri = cred['uri'].encode('utf8')
	  elif key.find('redis') > 0:
	    mq_info = vcap_services[key][0]
		
	    cred = mq_info['credentials']
	    urimq = cred['uri'].encode('utf8')

  app.config['SQLALCHEMY_DATABASE_URI'] = uri 
  mq = redis.StrictRedis.from_url(urimq + '/0')
else:
  app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:password@mysql:3306/sys'
  mq = redis.StrictRedis(host=os.getenv('MQ_HOST', 'localhost'), port=os.getenv('MQ_PORT', 9003), db=0)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class searchrow(db.Model):
  __tablename__ = "searchqueue"

  sessionid = db.Column(db.String(512), primary_key = True)
  hotelid = db.Column(db.Integer)

  def __init__(self, sessionid, hotelid):
    self.sessionid = sessionid
    self.hotelid = hotelid

  def __repr__(self):
    return '<Row %r %r>' % self.sessionid, self.hotelid

def callback():
  sub = mq.pubsub()
  sub.subscribe('searchqueue')

  while True:
    for message in sub.listen():
      if message['type'] == 'message':
        data = message['data']
        data = json.loads(data)
        
        sessionid = data['sessionid']
        print 'Recieved: {0}'.format(sessionid)

        searchrows = []
        for searchid in data['searchids']:
          searchrows.append(searchrow(sessionid, searchid))

        db.session.add_all(searchrows);
        db.session.commit()
        print 'Populated deals data store ...'
        mq.publish('searchstatus', sessionid)
        sys.stdout.flush()


def searchqueue():
  t = threading.Thread(target = callback)
  t.setDaemon(True)
  
  t.start()
  while True:
    time.sleep(30)

if __name__ == "__main__":
  searchqueue()
