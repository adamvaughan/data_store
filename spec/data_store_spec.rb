require 'spec_helper'

describe "DataStore" do
  let(:uuid) { 'b731a7d0-774b-012f-582a-482a14096e91' }

  before(:all) do
    @pid = fork do
      DataStore::Server.start('0.0.0.0', 13490) do |server|
        server.data_directory = '/tmp/data_store'
        server.max_days_per_file = 30
      end
    end

    sleep 0.5
  end

  before(:each) do
    FileUtils.rm_rf('/tmp/data_store')
  end

  after(:all) do
    Process.kill 'TERM', @pid
  end

  describe "inserting a record into a new file" do
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_001" }
    let(:file) { DataStore::DataFile.new(path) }

    before do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_time => Time.at(100201), :record_value => 0.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)
      end
    end

    it "creates a new file" do
      File.should exist(path)
    end

    it "sets the header values" do
      file.start_day.should eq(1)
      file.end_day.should eq(1)
      file.start_time.should eq(Time.at(100201))
      file.end_time.should eq(Time.at(100201))
      file.close
    end

    it "writes the record" do
      records = file.records.map { |record| record }
      records.should have(1).item
      records.first.time.should eq(Time.at(100201))
      records.first.value.should eq(0.5)
      file.close
    end

    it "returns the record" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(100201)), 0
        socket.recv(4).unpack('L').first.should eq(1)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)
      end
    end
  end

  describe "appending a record to an existing file" do
    let(:path) { '/tmp/data_store/1970/b7/31/b731a7d0-774b-012f-582a-482a14096e91_001_001' }
    let(:file) { DataStore::DataFile.new(path) }

    before do
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |file|
        file.write [100201, 100201].pack('LL')
        file.write [100201, 0.5].pack('LF')
      end

      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_time => Time.at(100202), :record_value => 1.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)
      end
    end

    it "updates the header values" do
      file.start_day.should eq(1)
      file.end_day.should eq(1)
      file.start_time.should eq(Time.at(100201))
      file.end_time.should eq(Time.at(100202))
      file.close
    end

    it "writes the record" do
      records = file.records.map { |record| record }
      records.should have(2).item
      records.first.time.should eq(Time.at(100201))
      records.first.value.should eq(0.5)
      records.last.time.should eq(Time.at(100202))
      records.last.value.should eq(1.5)
      file.close
    end

    it "returns the record" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(100202)), 0
        socket.recv(4).unpack('L').first.should eq(2)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100202))
        record.value.should eq(1.5)
      end
    end
  end

  describe "inserting records out of order" do
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_001" }

    before do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_time => Time.at(100201), :record_value => 0.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(100203), :record_value => 1.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(100202), :record_value => 2.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)
      end
    end

    it "creates new files for records that can not be appended to an existing file" do
      File.should exist(path)
      File.should exist("#{path}_1")
    end

    it "inserts all data in order" do
      file = DataStore::DataFile.new(path)
      records = file.records.map { |record| record }
      records.should have(2).item
      records.first.time.should eq(Time.at(100201))
      records.first.value.should eq(0.5)
      records.last.time.should eq(Time.at(100203))
      records.last.value.should eq(1.5)
      file.close

      file = DataStore::DataFile.new("#{path}_1")
      records = file.records.map { |record| record }
      records.should have(1).item
      records.first.time.should eq(Time.at(100202))
      records.first.value.should eq(2.5)
      file.close
    end

    it "returns all data in order" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(100203)), 0
        socket.recv(4).unpack('L').first.should eq(3)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100202))
        record.value.should eq(2.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100203))
        record.value.should eq(1.5)
      end
    end
  end

  describe "inserting records from multiple days" do
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_001" }

    before do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_time => Time.at(100201), :record_value => 0.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(1000201), :record_value => 1.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)
      end
    end

    it "renames the file to match the day range" do
      File.should_not exist(path)
      File.should exist(path.sub(/_001$/, '_012'))
    end

    it "returns all data" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(1000201)), 0
        socket.recv(4).unpack('L').first.should eq(2)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(1000201))
        record.value.should eq(1.5)
      end
    end
  end

  describe "reading a subset of records" do
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_001" }

    before do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_time => Time.at(100201), :record_value => 0.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(100202), :record_value => 1.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(100203), :record_value => 2.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)

        socket.send request_data(:put, :record_time => Time.at(100204), :record_value => 3.5), 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(1)
      end
    end

    it "returns only the requested records" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100202), :end_time => Time.at(100203)), 0
        socket.recv(4).unpack('L').first.should eq(2)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100202))
        record.value.should eq(1.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100203))
        record.value.should eq(2.5)
      end
    end

    it "returns 0 when there are no matching records" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100205), :end_time => Time.at(100205)), 0
        socket.recv(4).unpack('L').first.should eq(0)
      end
    end
  end

  describe "inserting multiple records" do
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_012" }
    let(:file) { DataStore::DataFile.new("/tmp/data_store/1970/b7/31/#{uuid}_001_001") }

    before do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        data = request_data(:put, :record_count => 2, :record_time => 0, :record_value => 0.0)
        data << [100201, 0.5].pack('LF')
        data << [1000202, 1.5].pack('LF')
        socket.send data, 0
        result = socket.recv(4)
        result.unpack('L').first.should eq(2)
      end
    end

    it "creates a new file" do
      File.should exist(path)
    end

    it "sets the header values" do
      file = DataStore::DataFile.new(path)
      file.start_day.should eq(1)
      file.end_day.should eq(12)
      file.start_time.should eq(Time.at(100201))
      file.end_time.should eq(Time.at(1000202))
      file.close
    end

    it "writes the record" do
      file = DataStore::DataFile.new(path)
      records = file.records.map { |record| record }
      records.should have(2).item
      records.first.time.should eq(Time.at(100201))
      records.first.value.should eq(0.5)
      records.last.time.should eq(Time.at(1000202))
      records.last.value.should eq(1.5)
      file.close
    end

    it "returns the record" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(1000202)), 0
        socket.recv(4).unpack('L').first.should eq(2)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(1000202))
        record.value.should eq(1.5)
      end
    end
  end

  describe "reading records across multiple years" do
    before do
      path = "/tmp/data_store/1970/b7/31/#{uuid}_001_001"
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |file|
        file.write [100201, 100201].pack('LL')
        file.write [100201, 0.5].pack('LF')
      end

      path = "/tmp/data_store/1973/b7/31/#{uuid}_062_062"
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |file|
        file.write [100000202, 100000202].pack('LL')
        file.write [100000202, 1.5].pack('LF')
      end
    end

    it "returns the records" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:get, :start_time => Time.at(100201), :end_time => Time.at(100000202)), 0
        socket.recv(4).unpack('L').first.should eq(2)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100201))
        record.value.should eq(0.5)

        record = DataStore::Record.new(*socket.recv(8).unpack('LF'))
        record.time.should eq(Time.at(100000202))
        record.value.should eq(1.5)
      end
    end
  end

  describe "storing more data than can fit in a single file" do
    before do
      path = "/tmp/data_store/1970/b7/31/#{uuid}_001_001"
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |file|
        file.write [100201, 100201].pack('LL')
        file.write [100201, 0.5].pack('LF')
      end

      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |file|
        file.write [100000202, 100000202].pack('LL')
        file.write [100000202, 1.5].pack('LF')
      end
    end

    it "stores the records in a new file" do
      TCPSocket.open('127.0.0.1', 13490) do |socket|
        socket.send request_data(:put, :record_count => 1, :record_time => Time.at(3000201), :record_value => 1.5), 0
        socket.recv(4).unpack('L').first.should eq(1)
      end

      file = DataStore::DataFile.new("/tmp/data_store/1970/b7/31/#{uuid}_035_035")
      records = file.records.map { |record| record }
      records.should have(1).item
      records.first.time.should eq(Time.at(3000201))
      records.first.value.should eq(1.5)
    end
  end
end
