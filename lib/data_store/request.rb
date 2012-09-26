module DataStore
  # A request to the data store server. There are two types of requests, GET and
  # PUT. GET requests have the following fields:
  #
  #     Type
  #     UUID
  #     Record Count # unused
  #     Start Time
  #     End Time
  #
  # PUT requests have the following fields:
  #
  #     Type
  #     UUID
  #     Record Count
  #     Record Time
  #     Record Value
  #
  # PUT requests come in two forms. When storing a single record, the record data
  # can be send with the request. In this case the record count should be set to
  # 1. If multiple records are being inserted at one time, record count should
  # indicate the number of records and all records should be sent after the request.
  class Request
    REQUEST_TYPE_MAP = { 1 => :get, 2 => :put }
    REQUEST_SIZE = 56
    RECORD_SIZE = 8

    attr_accessor :data

    def initialize(data = nil)
      @data = data
    end

    def <<(data)
      @data ||= ''
      @data << data
    end

    def type
      raise "Insufficient data for determining the request type" unless received?(4)
      @type ||= (REQUEST_TYPE_MAP[@data[0..3].unpack('L').first] || :unknown)
    end

    def uuid
      raise "Insufficient data for determining the request uuid" unless received?(40)
      @uuid ||= @data[4..39]
    end

    def record_count
      raise "Record count is only applicable for put requests" unless put?
      @record_count ||= @data[44..47].unpack('L').first
    end

    def start_time
      raise "Start time is only applicable for get requests" unless get?
      @start_time ||= Time.at(@data[48..51].unpack('L').first)
    end

    def end_time
      raise "End time is only applicable for get requests" unless get?
      @end_time ||= Time.at(@data[52..-1].unpack('L').first)
    end

    def range
      @range ||= (start_time..end_time)
    end

    def record_time
      raise "Record time is only applicable for put requests" unless put?
      @record_time ||= Time.at(@data[48..51].unpack('L').first)
    end

    def record_value
      raise "Record value is only applicable for put requests" unless put?
      @record_value ||= @data[52..-1].unpack('F').first
    end

    # Determines if the entire request has been received.
    def complete?
      if received?(REQUEST_SIZE)
        if put?
          if record_count == 1
            true
          else
            received?(REQUEST_SIZE + (RECORD_SIZE * record_count))
          end
        else
          true
        end
      else
        false
      end
    end

    def get?
      type == :get
    end

    def put?
      type == :put
    end

    # Determines if the given number of bytes have been received.
    def received?(bytes)
      @data && @data.length >= bytes
    end

    # Gets the records included with the request. This only applies to PUT requests.
    def records
      raise "Records is only applicable for put requests" unless put?
      records = []

      if record_count == 1
        records << Record.new(record_time, record_value)
      else
        record_count.times do |i|
          offset = REQUEST_SIZE + (RECORD_SIZE * i)
          record_data = @data[offset..(offset + RECORD_SIZE)]
          records << Record.new(*record_data.unpack('LF'))
        end
      end

      records
    end
  end
end
