module DataStore
  class Server
    # Starts the data store server. The configuration is yielded if a block is
    # given to allow easy configuration.
    #
    # Example:
    #     DataStore.start('127.0.0.1', 3490) do |config|
    #       config.data_directory = '/tmp/data_store'
    #     end
    def self.start(host, port)
      yield DataStore.config if block_given?

      puts 'Starting data store'
      puts "Listening on #{host}:#{port}"

      if RUBY_PLATFORM =~ /darwin/
        EventMachine.kqueue
      else
        EventMachine.epoll
      end

      EventMachine.run do
        trap('TERM') { stop }
        trap('INT') { stop }

        begin
          EventMachine.start_server(host, port, Connection)
        rescue => e
          puts "Error starting server - #{e.message}"
          stop
        end
      end
    end

    def self.stop
      puts
      puts 'Stopping data store'
      EventMachine.stop
      exit
    end
  end
end
