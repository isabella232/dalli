# frozen_string_literal: true
require 'socket'
require 'timeout'

module Dalli
  class Server
    attr_accessor :hostname
    attr_accessor :port
    attr_accessor :weight
    attr_accessor :options
    attr_reader :sock
    attr_reader :socket_type  # possible values: :unix, :tcp

    DEFAULT_PORT = 11211
    DEFAULT_WEIGHT = 1
    DEFAULTS = {
      # seconds between trying to contact a remote server
      :down_retry_delay => 60,
      # connect/read/write timeout for socket operations
      :socket_timeout => 0.5,
      # times a socket operation may fail before considering the server dead
      :socket_max_failures => 2,
      # amount of time to sleep between retries when a failure occurs
      :socket_failure_delay => 0.01,
      # max size of value in bytes (default is 1 MB, can be overriden with "memcached -I <size>")
      :value_max_bytes => 1024 * 1024,
      # surpassing value_max_bytes either warns (false) or throws (true)
      :error_when_over_max_size => false,
      :compressor => Compressor,
      # min byte size to attempt compression
      :compression_min_size => 1024,
      # max byte size for compression
      :compression_max_size => false,
      :serializer => Marshal,
      :username => nil,
      :password => nil,
      :keepalive => true,
      # max byte size for SO_SNDBUF
      :sndbuf => nil,
      # max byte size for SO_RCVBUF
      :rcvbuf => nil
    }

    def initialize(attribs, options = {})
      @hostname, @port, @weight, @socket_type = parse_hostname(attribs)
      @fail_count = 0
      @down_at = nil
      @last_down_at = nil
      @options = DEFAULTS.merge(options)
      @sock = nil
      @msg = nil
      @error = nil
      @pid = nil
      @inprogress = nil
    end

    def name
      if socket_type == :unix
        hostname
      else
        "#{hostname}:#{port}"
      end
    end

    # Chokepoint method for instrumentation
    def request(op, *args)
      verify_state
      raise Dalli::NetworkError, "#{name} is down: #{@error} #{@msg}. If you are sure it is running, ensure memcached version is > 1.4." unless alive?
      begin
        send(op, *args)
      rescue Dalli::MarshalError => ex
        Dalli.logger.error "Marshalling error for key '#{args.first}': #{ex.message}"
        Dalli.logger.error "You are trying to cache a Ruby object which cannot be serialized to memcached."
        Dalli.logger.error ex.backtrace.join("\n\t")
        false
      rescue Dalli::DalliError, Dalli::NetworkError, Dalli::ValueOverMaxSize, Timeout::Error
        raise
      rescue => ex
        Dalli.logger.error "Unexpected exception during Dalli request: #{ex.class.name}: #{ex.message}"
        Dalli.logger.error ex.backtrace.join("\n\t")
        down!
      end
    end

    def alive?
      return true if @sock

      if @last_down_at && @last_down_at + options[:down_retry_delay] >= Time.now
        time = @last_down_at + options[:down_retry_delay] - Time.now
        Dalli.logger.debug { "down_retry_delay not reached for #{name} (%.3f seconds left)" % time }
        return false
      end

      connect
      !!@sock
    rescue Dalli::NetworkError
      false
    end

    def close
      return unless @sock
      @sock.close rescue nil
      @sock = nil
      @pid = nil
      @inprogress = false
    end

    def lock!
    end

    def unlock!
    end

    def serializer
      @options[:serializer]
    end

    def compressor
      @options[:compressor]
    end

    # NOTE: Additional public methods should be overridden in Dalli::Threadsafe

    private

    def verify_state
      failure!(RuntimeError.new('Already writing to socket')) if @inprogress
      if @pid && @pid != Process.pid
        message = 'Fork detected, re-connecting child process...'
        Dalli.logger.info { message }
        reconnect! message
      end
    end

    def reconnect!(message)
      close
      sleep(options[:socket_failure_delay]) if options[:socket_failure_delay]
      raise Dalli::NetworkError, message
    end

    def failure!(exception)
      message = "#{name} failed (count: #{@fail_count}) #{exception.class}: #{exception.message}"
      Dalli.logger.warn { message }

      @fail_count += 1
      if @fail_count >= options[:socket_max_failures]
        down!
      else
        reconnect! 'Socket operation failed, retrying...'
      end
    end

    def down!
      close

      @last_down_at = Time.now

      if @down_at
        time = Time.now - @down_at
        Dalli.logger.debug { "#{name} is still down (for %.3f seconds now)" % time }
      else
        @down_at = @last_down_at
        Dalli.logger.warn { "#{name} is down" }
      end

      @error = $! && $!.class.name
      @msg = @msg || ($! && $!.message && !$!.message.empty? && $!.message)
      raise Dalli::NetworkError, "#{name} is down: #{@error} #{@msg}"
    end

    def up!
      if @down_at
        time = Time.now - @down_at
        Dalli.logger.warn { "#{name} is back (downtime was %.3f seconds)" % time }
      end

      @fail_count = 0
      @down_at = nil
      @last_down_at = nil
      @msg = nil
      @error = nil
    end

    def multi?
      Thread.current[:dalli_multi]
    end

    def get(key, options=nil)
      command = %w(mg) << key << "v" << "f"
      write_command(["mg", key, "v", "f"])
      read_response(unpack: true, cache_nils: options && options[:cache_nils])
    end

    def send_multiget(keys)
      raise NotImplementedError
    end

    def set(key, value, ttl, cas, options, mode: nil)
      (value, flags) = serialize(key, value, options)
      ttl = sanitize_ttl(ttl)
      _raw_set(key, value, ttl: ttl, cas: cas, flags: flags, mode: mode)
    end

    def _raw_set(key, value, ttl: nil, cas: nil, mode: nil, flags: nil)
      command = ["ms", key, "S#{value.bytesize}"]
      command << "M#{mode}" if mode
      command << "C#{cas}" if cas && cas.to_i > 0
      command << "T#{ttl}" if ttl
      command << "F#{flags}" if flags

      write_command(command, value)
      read_response
    end

    def add(key, value, ttl, options)
      set(key, value, ttl, nil, options, mode: 'E')
    end

    def replace(key, value, ttl, cas, options)
      set(key, value, ttl, nil, options, mode: 'R')
    end

    def delete(key, cas)
      command = %w(md) << key
      command << "C#{cas}" if cas > 0
      write_command(command)
      read_response
    end

    def flush(ttl)
      command = %w(flush_all)
      command << ttl.to_i.to_s if ttl
      write_command(command)
      read_response
    end

    def decr_incr(mode, key, count, ttl, default)
      ttl = sanitize_ttl(ttl)
      command = %w(ma) << key << "v" << "M#{mode}" << "D#{count}" << "T#{ttl}"
      command << "J#{default}" << "N#{ttl}" if default

      write_command(command)
      value = read_response
      value ? value.to_i : value
    end

    def decr(key, count, ttl, default)
      decr_incr('-', key, count, ttl, default)
    end

    def incr(key, count, ttl, default)
      decr_incr('+', key, count, ttl, default)
    end

    # Noop is a keepalive operation but also used to demarcate the end of a set of pipelined commands.
    # We need to read all the responses at once.
    def noop
      raise NotImplementedError
    end

    def append(key, value)
      _raw_set(key, value, mode: 'A')
    end

    def prepend(key, value)
      _raw_set(key, value, mode: 'P')
    end

    def stats(info='')
      command = %w(stats)
      command << info unless info.nil? || info.empty?
      write_command(command)
      read_response
    end

    def reset_stats
      stats('reset')
    end

    def cas(key)
      write_command(["mg", key, "v", "c", "f"])
      read_response(unpack: true, flag: "c")
    end

    def version
      write_command %w(version)
      read_response
    end

    def touch(key, ttl)
      ttl = sanitize_ttl(ttl)
      write_command(['mg', key, "T#{ttl}"])
      read_response
    end

    # http://www.hjp.at/zettel/m/memcached_flags.rxml
    # Looks like most clients use bit 0 to indicate native language serialization
    # and bit 1 to indicate gzip compression.
    FLAG_SERIALIZED = 0x1
    FLAG_COMPRESSED = 0x2

    def serialize(key, value, options=nil)
      marshalled = false
      value = unless options && options[:raw]
        marshalled = true
        begin
          self.serializer.dump(value)
        rescue Timeout::Error => e
          raise e
        rescue => ex
          # Marshalling can throw several different types of generic Ruby exceptions.
          # Convert to a specific exception so we can special case it higher up the stack.
          exc = Dalli::MarshalError.new(ex.message)
          exc.set_backtrace ex.backtrace
          raise exc
        end
      else
        value.to_s
      end
      compressed = false
      set_compress_option = true if options && options[:compress]
      if (@options[:compress] || set_compress_option) && value.bytesize >= @options[:compression_min_size] &&
        (!@options[:compression_max_size] || value.bytesize <= @options[:compression_max_size])
        value = self.compressor.compress(value)
        compressed = true
      end

      flags = 0
      flags |= FLAG_COMPRESSED if compressed
      flags |= FLAG_SERIALIZED if marshalled
      [value, flags]
    end

    def deserialize(value, flags)
      value = self.compressor.decompress(value) if (flags & FLAG_COMPRESSED) != 0
      value = self.serializer.load(value) if (flags & FLAG_SERIALIZED) != 0
      value
    rescue TypeError
      raise if $!.message !~ /needs to have method `_load'|exception class\/object expected|instance of IO needed|incompatible marshal file format/
      raise UnmarshalError, "Unable to unmarshal value: #{$!.message}"
    rescue ArgumentError
      raise if $!.message !~ /undefined class|marshal data too short/
      raise UnmarshalError, "Unable to unmarshal value: #{$!.message}"
    rescue NameError
      raise if $!.message !~ /uninitialized constant/
      raise UnmarshalError, "Unable to unmarshal value: #{$!.message}"
    rescue Zlib::Error
      raise UnmarshalError, "Unable to uncompress value: #{$!.message}"
    end

    def guard_max_value(key, value)
      if value.bytesize <= @options[:value_max_bytes]
        yield
      else
        message = "Value for #{key} over max size: #{@options[:value_max_bytes]} <= #{value.bytesize}"
        raise Dalli::ValueOverMaxSize, message if @options[:error_when_over_max_size]

        Dalli.logger.error "#{message} - this value may be truncated by memcached"
        false
      end
    end

    # https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L79
    # > An expiration time, in seconds. Can be up to 30 days. After 30 days, is treated as a unix timestamp of an exact date.
    MAX_ACCEPTABLE_EXPIRATION_INTERVAL = 30*24*60*60 # 30 days
    def sanitize_ttl(ttl)
      ttl_as_i = ttl.to_i
      return ttl_as_i if ttl_as_i <= MAX_ACCEPTABLE_EXPIRATION_INTERVAL
      now = Time.now.to_i
      return ttl_as_i if ttl_as_i > now # already a timestamp
      Dalli.logger.debug "Expiration interval (#{ttl_as_i}) too long for Memcached, converting to an expiration timestamp"
      now + ttl_as_i
    end

    # Implements the NullObject pattern to store an application-defined value for 'Key not found' responses.
    class NilObject; end
    NOT_FOUND = NilObject.new

    def generic_response(unpack=false, cache_nils=false)
      (extras, _, status, count) = read_header.unpack(NORMAL_HEADER)
      data = read(count) if count > 0
      if status == 1
        cache_nils ? NOT_FOUND : nil
      elsif status == 2 || status == 5
        false # Not stored, normal status for add operation
      elsif status != 0
        raise Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}"
      elsif data
        flags = data[0...extras].unpack('N')[0]
        value = data[extras..-1]
        unpack ? deserialize(value, flags) : value
      else
        true
      end
    end

    def cas_response
      (_, _, status, count, _, cas) = read_header.unpack(CAS_HEADER)
      read(count) if count > 0  # this is potential data that we don't care about
      if status == 1
        nil
      elsif status == 2 || status == 5
        false # Not stored, normal status for add operation
      elsif status != 0
        raise Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}"
      else
        cas
      end
    end

    def keyvalue_response
      hash = {}
      while true
        (key_length, _, body_length, _) = read_header.unpack(KV_HEADER)
        return hash if key_length == 0
        key = read(key_length)
        value = read(body_length - key_length) if body_length - key_length > 0
        hash[key] = value
      end
    end

    def multi_response
      hash = {}
      while true
        (key_length, _, body_length, _) = read_header.unpack(KV_HEADER)
        return hash if key_length == 0
        flags = read(4).unpack('N')[0]
        key = read(key_length)
        value = read(body_length - key_length - 4) if body_length - key_length - 4 > 0
        hash[key] = deserialize(value, flags)
      end
    end

    def write_command(args, body = nil)
      buffer = "".b
      buffer << args.join(' ') << "\r\n"
      buffer << body << "\r\n" if body
      write(buffer)
    end

    def write(bytes)
      begin
        @inprogress = true
        result = @sock.write(bytes)
        @inprogress = false
        result
      rescue SystemCallError, Timeout::Error => e
        failure!(e)
      end
    end

    def read_response(unpack: false, cache_nils: false, flag: nil)
      elements = readline.split
      type = elements.shift

      case type
      when 'OK'
        if flag
          [true, extract_flag(elements, flag)]
        else
          true
        end
      when 'RESET'
        true
      when 'EN' # Miss
        nil
      when 'NF' # Not Found
        nil
      when 'NS' # Not Set
        false
      when 'VA'
        bytesize = elements.shift.to_i
        value = read(bytesize + 2)
        value.chomp!

        if unpack
          value = deserialize(value, extract_flag(elements, 'f').to_i)
        end

        if flag
          [value, extract_flag(elements, flag)]
        else
          value
        end
      when 'VERSION'
        elements.first
      when 'CLIENT_ERROR'
        raise DalliError, elements.join(' ')
      when 'SERVER_ERROR'
        if !@options[:error_when_over_max_size] && elements.join(' ') == "object too large for cache"
          false
        else
          raise DalliError, elements.join(' ')
        end
      when 'STAT'
        stats = {}
        elements.unshift(type)
        while elements.first == 'STAT'
          stats[elements[1]] = elements[2]
          elements = readline.split
        end
        stats
      else
        raise NotImplementedError, "Unknown response type: #{type.inspect} #{elements.join(' ')}"
      end
    end

    def extract_flag(elements, flag_name)
      flag = elements.find { |e| e.start_with?(flag_name) }
      return unless flag
      flag[1..-1]
    end

    def readline
      begin
        @inprogress = true
        data = @sock.gets
        @inprogress = false
        data
      rescue SystemCallError, Timeout::Error, EOFError => e
        failure!(e)
      end
    end

    def read(count)
      begin
        @inprogress = true
        data = @sock.readfull(count)
        @inprogress = false
        data
      rescue SystemCallError, Timeout::Error, EOFError => e
        failure!(e)
      end
    end

    def connect
      Dalli.logger.debug { "Dalli::Server#connect #{name}" }

      begin
        @pid = Process.pid
        if socket_type == :unix
          @sock = KSocket::UNIX.open(hostname, self, options)
        else
          @sock = KSocket::TCP.open(hostname, port, self, options)
        end
        sasl_authentication if need_auth?
        @version = version # trigger actual connect
        up!
      rescue Dalli::DalliError # SASL auth failure
        raise
      rescue SystemCallError, Timeout::Error, EOFError, SocketError => e
        # SocketError = DNS resolution failure
        failure!(e)
      end
    end

    # Response codes taken from:
    # https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#response-status
    RESPONSE_CODES = {
      0 => 'No error',
      1 => 'Key not found',
      2 => 'Key exists',
      3 => 'Value too large',
      4 => 'Invalid arguments',
      5 => 'Item not stored',
      6 => 'Incr/decr on a non-numeric value',
      7 => 'The vbucket belongs to another server',
      8 => 'Authentication error',
      9 => 'Authentication continue',
      0x20 => 'Authentication required',
      0x81 => 'Unknown command',
      0x82 => 'Out of memory',
      0x83 => 'Not supported',
      0x84 => 'Internal error',
      0x85 => 'Busy',
      0x86 => 'Temporary failure'
    }

    #######
    # SASL authentication support for NorthScale
    #######

    def need_auth?
      @options[:username] || ENV['MEMCACHE_USERNAME']
    end

    def username
      @options[:username] || ENV['MEMCACHE_USERNAME']
    end

    def password
      @options[:password] || ENV['MEMCACHE_PASSWORD']
    end

    def sasl_authentication
      Dalli.logger.info { "Dalli/SASL authenticating as #{username}" }
      raise NotImplementedError
    end

    def parse_hostname(str)
      res = str.match(/\A(\[([\h:]+)\]|[^:]+)(?::(\d+))?(?::(\d+))?\z/)
      raise Dalli::DalliError, "Could not parse hostname #{str}" if res.nil? || res[1] == '[]'
      hostnam = res[2] || res[1]
      if hostnam =~ /\A\//
        socket_type = :unix
        # in case of unix socket, allow only setting of weight, not port
        raise Dalli::DalliError, "Could not parse hostname #{str}" if res[4]
        weigh = res[3]
      else
        socket_type = :tcp
        por = res[3] || DEFAULT_PORT
        por = Integer(por)
        weigh = res[4]
      end
      weigh ||= DEFAULT_WEIGHT
      weigh = Integer(weigh)
      return hostnam, por, weigh, socket_type
    end
  end
end
