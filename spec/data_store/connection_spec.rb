require 'spec_helper'

module DataStore
  describe Connection do
    let(:connection) { Object.new }

    before do
      connection.extend Connection
    end

    describe "#receive_data" do
      context "when a partial request is received" do
        it "does not take any action" do
          connection.should_not_receive(:handle_get_request)
          connection.should_not_receive(:handle_put_request)
          connection.should_not_receive(:handle_unknown_request)
          connection.receive_data 'test'
          connection.request.data.should eq('test')
        end
      end

      context "when a put request is received" do
        it "handles the put request" do
          connection.should_receive(:handle_put_request)
          connection.should_not_receive(:handle_get_request)
          connection.should_not_receive(:handle_unknown_request)
          connection.receive_data request_data(:put)
          connection.request.should be_nil
        end
      end

      context "when a get request is received" do
        it "handles the get request" do
          connection.should_receive(:handle_get_request)
          connection.should_not_receive(:handle_put_request)
          connection.should_not_receive(:handle_unknown_request)
          connection.receive_data request_data(:get)
          connection.request.should be_nil
        end
      end

      context "when an unknown request is received" do
        it "handles the unknown request" do
          connection.should_receive(:handle_unknown_request)
          connection.should_not_receive(:handle_get_request)
          connection.should_not_receive(:handle_put_request)
          connection.receive_data request_data(:unknown)
          connection.request.should be_nil
        end
      end
    end

    describe "#handle_get_request" do
      context "when no data files match" do
        it "doesn't send any records back to the client" do
          DataFile.should_receive(:containing).and_return([])
          connection.should_receive(:send_data).with([0].pack('L'))
          connection.request = Request.new(request_data(:get))
          connection.handle_get_request
        end
      end

      context "when files are found that match" do
        it "returns the matching records" do
          record = Record.new(Time.at(100121), 0.5)
          file = DataFile.new('/tmp/2012/b7/31/b731a7d0-774b-012f-582a-482a14096e91_120_121')
          file.should_receive(:records).and_return([record])
          DataFile.should_receive(:containing).and_return([file])
          connection.should_receive(:send_data).with([1].pack('L'))
          connection.should_receive(:send_data).with([100121, 0.5].pack('LF'))
          connection.request = Request.new(request_data(:get, :start_time => Time.at(100120), :end_time => Time.at(100122)))
          connection.handle_get_request
        end
      end
    end

    describe "#handle_put_request" do
      context "when a single record is provided" do
        it "stores the record" do
          file = DataFile.new('/tmp/2012/b7/31/b731a7d0-774b-012f-582a-482a14096e91_120_121')
          file.should_receive(:<<).with(Record.new(Time.at(100121), 0.5))
          DataFile.should_receive(:storing).and_return(file)
          connection.should_receive(:send_data).with([1].pack('L'))
          connection.request = Request.new(request_data(:put, :record_time => Time.at(100121), :record_value => 0.5))
          connection.handle_put_request
        end
      end

      context "when multiple records are provided" do
        it "stores the records" do
          file = DataFile.new('/tmp/2012/b7/31/b731a7d0-774b-012f-582a-482a14096e91_120_121')
          file.should_receive(:<<).with(Record.new(Time.at(100120), 0.5))
          file.should_receive(:<<).with(Record.new(Time.at(100121), 1.5))
          DataFile.should_receive(:storing).twice.and_return(file)
          connection.should_receive(:send_data).with([2].pack('L'))
          connection.request = Request.new(request_data(:put, :record_count => 2, :record_time => 0, :record_value => 0.5))
          connection.request << [100120, 0.5].pack('LF')
          connection.request << [100121, 1.5].pack('LF')
          connection.handle_put_request
        end
      end
    end

    describe "#handle_unknown_request" do
      it "sends an error response" do
        connection.should_receive(:send_data).with([-1].pack('L'))
        connection.request = Request.new
        connection.handle_unknown_request
      end
    end
  end
end
