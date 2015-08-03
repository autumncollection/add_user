#!/usr/bin/env ruby
#encoding:utf-8
require 'bundler/setup'
require 'csv'
require 'elasticsearch'
require 'redis'

file   = File.open('/home/tom/Downloads/instagram_cs.csv')
shift  = 100
@es    = Elasticsearch::Client.new host: 'localhost:9996'
@index = 'instagram_users'
@rdb   = Redis.new host: 'localhost', port: '9995'
@rdb.select 14
counter = 0
users   = {}
@saved  = 0
@query   = %Q{
  {
    "fields": [],
    "query": {
      "filtered": {
        "filter": {
          "ids" :{
            "type": "user",
            "values": ###
          }
        }
      }
    }
  }
}

def add_users users
  users_es = @es.search(index: @index, body: @query.sub('###', "#{users.keys}"))
  users_es = users_es['hits']['hits'].map { |user|
    user['_id'].to_s
  }
  users_add = users.keys - (users_es)
  users_add.each { |user_id|
    @rdb.set(users[user_id], "False")
  }
  @saved += users_add.count
end

CSV(file).each { |line|
  counter += 1
  next if counter == 1
  users["#{line[-2]}"] = line[1]
  if counter % shift == 0
    add_users users
    users = {}
  end
}
add_users users

p "New: #{@saved} vs #{counter}"