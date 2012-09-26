module DataStore
  # A client connection to the server.
  module Connection
    attr_accessor :request

    ERROR_RESPONSE = [-1].pack('L')

    def receive_data(data)
      @request ||= Request.new
      @request << data

      if @request.complete?
        case @request.type
        when :get then handle_get_request
        when :put then handle_put_request
        else handle_unknown_request
        end

        @request = nil
      end
    rescue => e
      puts "Error receiving data - #{e.message}"
      close_connection
    end

    # Handles a GET request. The response starts with the number of matching records
    # found followed by the records.
    def handle_get_request
      files = DataFile.containing(@request.uuid, @request.range)
      records = []

      files.each do |file|
        file.records.each do |record|
          records << record if @request.range.cover?(record.time)
        end

        file.close
      end

      send_data [records.length].pack('L')

      records.sort.each do |record|
        send_data [record.time.to_i, record.value].pack('LF')
      end
    end

    # Handles a PUT request. The response is the number of records that were stored.
    def handle_put_request
      @request.records.each do |record|
        file = DataFile.storing(@request.uuid, record.time)
        file << record
        file.close
      end

      send_data [@request.record_count].pack('L')
    end

    def handle_unknown_request
      send_data ERROR_RESPONSE
    end
  end
end
