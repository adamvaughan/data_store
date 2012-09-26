require 'spec_helper'

module DataStore
  describe Record do
    describe "#<=>" do
      context "when the record time is before the other record time" do
        it "returns -1" do
          first = Record.new(Time.at(12345), 0.1)
          second = Record.new(Time.at(12346), 0.2)
          (first <=> second).should eq(-1)
        end
      end

      context "when the record time is after the other record time" do
        it "returns 1" do
          first = Record.new(Time.at(12346), 0.1)
          second = Record.new(Time.at(12345), 0.2)
          (first <=> second).should eq(1)
        end
      end

      context "when the record times are the same" do
        it "returns 0" do
          first = Record.new(Time.at(12345), 0.1)
          second = Record.new(Time.at(12345), 0.2)
          (first <=> second).should be_zero
        end
      end
    end

    describe "#==" do
      context "when the record time and value equal the other record time and value" do
        it "returns false" do
          first = Record.new(Time.at(12345), 0.1)
          second = Record.new(Time.at(12346), 0.2)
          (first == second).should be_false
        end
      end

      context "when the record time and value matches the other record time and value" do
        it "returns true" do
          first = Record.new(Time.at(12345), 0.5)
          second = Record.new(Time.at(12345), 0.5)
          (first == second).should be_true
        end
      end
    end
  end
end
