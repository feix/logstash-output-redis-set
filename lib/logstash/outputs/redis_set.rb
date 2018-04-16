# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# An redis output that does nothing.
class LogStash::Outputs::Redis < LogStash::Outputs::Base
  config_name "redis_set"

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Connection timeout
  config :timeout, :validate => :number, :default => 5
  
  # Interval for reconnecting to failed Redis connections
  config :reconnect_interval, :validate => :number, :default => 1

  # The action of a redis output
  config :action, :validate => [ "SET", "SADD", "HSET", "ZADD" , "FTADD"], :required => true, :default => "SET"

  # The name of a redis key
  config :key, :validate => :string, :required => true

  # The value of a redis key
  config :value, :validate => :string, :require => false

  # The name of a redis key
  config :score, :validate => :string, :require => false
  config :member, :validate => :string, :require => false
  config :field, :validate => :string, :require => false

  # The id of a redisearch document
  config :docId, :validate => :string, :require => false

  public
  def register
    require 'redis'
    require 'redisearch-rb'
    @redis = nil
  end # def register

  public
  def receive(event)
    begin
      key = event.sprintf(@key)
      @redis ||= connect
      case @action
      when "ZADD"
        member = event.sprintf(@member)
        if @score
          score = event.sprintf(@score)
        else
          score = (event.get("@timestamp").to_f * 1000).to_i
        end
        @redis.zadd(key, score, member)
      when "SET"
        value = event.sprintf(@value)
        @redis.set(key, value)
      when "SADD"
        member = event.sprintf(@member)
        @redis.sadd(key, member)
      when "HSET"
        field = event.sprintf(@field)
        value = event.sprintf(@value)
        @redis.hset(key, field, value)
      when "FTADD"
        redisearch_client = RedisSearch.new(key, @redis)
        
        docFields = [];
        
        redisearch_client.info().each do | key, value |
          if value.class == Array
            if key == "fields"
              values.each do |item|
                docFields.push(item[0])
                docFields.push(event.get(item[0]))
              end
            end
          end
        end

        doc = [event.sprintf(@docId), docFields]
        redisearch_client.add_docs(doc, {replace: true})
      end
    rescue => e
      @logger.warn("Failed to set event to Redis", :event => event,
                   :identity => identity, :exception => e,
                   :backtrace => e.backtrace)
      sleep @reconnect_interval
      @redis = nil
      retry
    end
  end # def event

  # A string used to identify a Redis instance in log messages
  def identity
    "redis://#{@password}@#{@host}:#{@port}/#{@db} #{@action} #{@key}"
  end

  private
  def connect
    params = {
      :host => @host,
      :port => @port,
      :timeout => @timeout,
      :db => @db,
    }
    @logger.debug("connection params", params)

    if @password
      params[:password] = @password.value
    end

    Redis.new(params)
  end
end # class LogStash::Outputs::Redis
