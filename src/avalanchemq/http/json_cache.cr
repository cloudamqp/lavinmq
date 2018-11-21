require "cache_hash"
require "http/server/handler"
require "json"
require "logger"

module AvalancheMQ
  class JSONCacheHandler
    include HTTP::Handler

    @cache = CacheHash(String).new(5.seconds)
    @mutex = Hash(String, Mutex).new { |h, k| h[k] = Mutex.new }

    def initialize(@log : Logger)
    end

    def purge
      @cache.purge
    end

    def call(context)
      user = context.authenticated_username?
      return call_next(context) unless context.request.path.starts_with?("/api/") && user
      if %w(GET HEAD OPTIONS).includes? context.request.method
        cache_request(user, context)
      else
        request(context)
      end
    end

    private def cache_request(user, context)
      key = "#{user}:#{context.request.path}"
      context.response.headers["Cache-Control"] = "private"
      response = @cache.get(key)
      return cached_response(context, key, response) if response
      @mutex[key].synchronize do
        response = @cache.get(key)
        next cached_response(context, key, response) if response
        body = String.build do |io|
          context.response.output = IO::MultiWriter.new(context.response.output, io,
            sync_close: true)
        end
        call_next(context)
        if (200..299).includes?(context.response.status_code) && !body.empty?
          @cache.set(key, body)
          context.response.headers["ETag"] = @cache.time(key).to_s
        end
      end
    end

    private def request(context)
      call_next(context)
      @cache.purge
      context.response.headers["Cache-Control"] = "no-cache"
    end

    private def cached_response(context, key, response)
      etag = @cache.time(key).to_s
      if context.request.headers["If-None-Match"]? == etag
        context.response.status_code = 304
      else
        context.response.headers["ETag"] = etag
        context.response.content_type = "application/json"
        context.response.status_code = 200
        context.response.print response
      end
    end
  end
end
