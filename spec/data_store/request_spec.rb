require 'spec_helper'

module DataStore
  describe Request do
    let(:request) { Request.new }

    describe "#<<" do
      it "appends data to the request" do
        request << 'test'
        request.data.should eq('test')

        request << 'this'
        request.data.should eq('testthis')
      end
    end

    describe "#type" do
      it "raises an error if the full request has not been received" do
        lambda { request.type }.should raise_error
      end

      it "returns :get for GET requests" do
        request.data = request_data(:get)
        request.type.should eq(:get)
      end

      it "returns :put for PUT requests" do
        request.data = request_data(:put)
        request.type.should eq(:put)
      end

      it "returns :unknown for unrecognized requests" do
        request.data = request_data(:unknown)
        request.type.should eq(:unknown)
      end
    end

    describe "#uuid" do
      it "raises an error if the full request has not been received" do
        lambda { request.uuid }.should raise_error
      end

      it "returns the uuid" do
        request.data = request_data(:get)
        request.uuid.should eq('b731a7d0-774b-012f-582a-482a14096e91')
      end
    end

    describe "#record_count" do
      it "raises an error if the request is not a put request" do
        request.data = request_data(:get)
        lambda { request.record_count }.should raise_error
      end

      it "returns the record count" do
        request.data = request_data(:put, :record_count => 5)
        request.record_count.should eq(5)
      end
    end

    describe "#start_time" do
      it "raises an error if the request is not a get request" do
        request.data = request_data(:put)
        lambda { request.start_time }.should raise_error
      end

      it "returns the start time" do
        request.data = request_data(:get, :start_time => 1010201)
        request.start_time.should eq(Time.at(1010201))
      end
    end

    describe "#end_time" do
      it "raises an error if the request is not a get request" do
        request.data = request_data(:put)
        lambda { request.end_time }.should raise_error
      end

      it "returns the end time" do
        request.data = request_data(:get, :end_time => 1010201)
        request.end_time.should eq(Time.at(1010201))
      end
    end

    describe "#range" do
      it "raises an error if the request is not a get request" do
        request.data = request_data(:put)
        lambda { request.range }.should raise_error
      end

      it "returns the time range" do
        request.data = request_data(:get, :start_time => 1010200, :end_time => 1010201)
        request.range.should eq((Time.at(1010200)..Time.at(1010201)))
      end
    end

    describe "#record_time" do
      it "raises an error if the request is not a put request" do
        request.data = request_data(:get)
        lambda { request.record_time }.should raise_error
      end

      it "gets the record time" do
        request.data = request_data(:put, :record_time => 101201)
        request.record_time.should eq(Time.at(101201))
      end
    end

    describe "#record_value" do
      it "raises an error if the request is not a put request" do
        request.data = request_data(:get)
        lambda { request.record_value }.should raise_error
      end

      it "returns the record value" do
        request.data = request_data(:put, :record_value => 1.5)
        request.record_value.should eq(1.5)
      end
    end

    describe "#complete?" do
      context "when a single record is being inserted" do
        it "returns false if the request data is not the correct length" do
          request.should_not be_complete
        end

        it "returns true if the request data is the correct length" do
          request.data = request_data(:put)
          request.should be_complete
        end
      end

      context "when multiple records are being inserted" do
        it "returns false if the request data is not the correct length" do
          request.data = request_data(:put, :record_count => 2, :record_time => 0, :record_value => 0.0)
          request << [100121, 0.5].pack('LF')
          request.should_not be_complete
        end

        it "returns true if the request data is the correct length" do
          request.data = request_data(:put, :record_count => 2, :record_time => 0, :record_value => 0.0)
          request << [100121, 0.5].pack('LF')
          request << [100121, 0.5].pack('LF')
          request.should be_complete
        end
      end
    end

    describe "#get?" do
      it "returns true if the request is a GET request" do
        request.data = request_data(:put)
        request.should_not be_get
      end

      it "returns false if the request is not a GET request" do
        request.data = request_data(:get)
        request.should be_get
      end
    end

    describe "#put?" do
      it "returns true if the request is a PUT request" do
        request.data = request_data(:get)
        request.should_not be_put
      end

      it "returns false if the request is not a PUT request" do
        request.data = request_data(:put)
        request.should be_put
      end
    end

    describe "#received?" do
      it "returns false if the given number of bytes of data has not been received" do
        request.received?(10).should be_false
        request << 'test'
        request.received?(10).should be_false
      end

      it "returns true if the given number of bytes of data has been received" do
        request << 'thisisonlyatest'
        request.received?(10).should be_true
      end
    end

    describe "#records" do
      it "raises an error if called on a get request" do
        request << request_data(:get)
        lambda { request.records }.should raise_error
      end

      context "when storing a single record" do
        it "returns the record" do
          request << request_data(:put, :record_count => 1, :record_time => Time.at(100201), :record_value => 0.5)
          request.records.should eq([Record.new(Time.at(100201), 0.5)])
        end
      end

      context "when storing multiple records" do
        it "returns all of the records" do
          request << request_data(:put, :record_count => 3)
          request << [100201, 0.5].pack('LF')
          request << [100202, 1.5].pack('LF')
          request << [100203, 2.5].pack('LF')
          request.records.should eq([Record.new(Time.at(100201), 0.5), Record.new(Time.at(100202), 1.5), Record.new(Time.at(100203), 2.5)])
        end
      end
    end
  end
end
