require "./spec_helper"
require "../src/lavinmq/queue_filter/predicate.cr"

module QueueFilterSpecHelper
  def self.headers(h)
    AMQ::Protocol::Table.new(h)
  end

  def self.props(headers : Hash(String, AMQ::Protocol::Field)? = nil) : AMQ::Protocol::Properties
    p = AMQ::Protocol::Properties.new
    if headers
      p.headers = AMQ::Protocol::Table.new(headers)
    end
    p
  end
end

describe LavinMQ::QueueFilter do
  describe LavinMQ::QueueFilter::Clause do
    describe "op: eq" do
      it "matches when header value equals clause value" do
        c = LavinMQ::QueueFilter::Clause.new("x-test", LavinMQ::QueueFilter::Op::Eq, "bad")
        c.match?(QueueFilterSpecHelper.headers({"x-test" => "bad"})).should be_true
      end

      it "does not match when header value differs" do
        c = LavinMQ::QueueFilter::Clause.new("x-test", LavinMQ::QueueFilter::Op::Eq, "bad")
        c.match?(QueueFilterSpecHelper.headers({"x-test" => "good"})).should be_false
      end

      it "does not match when header missing" do
        c = LavinMQ::QueueFilter::Clause.new("x-test", LavinMQ::QueueFilter::Op::Eq, "bad")
        c.match?(QueueFilterSpecHelper.headers({"other" => "bad"})).should be_false
      end

      it "does not match when headers nil" do
        c = LavinMQ::QueueFilter::Clause.new("x-test", LavinMQ::QueueFilter::Op::Eq, "bad")
        c.match?(nil).should be_false
      end

      it "compares non-string values by string form" do
        c = LavinMQ::QueueFilter::Clause.new("x-retry", LavinMQ::QueueFilter::Op::Eq, "5")
        c.match?(QueueFilterSpecHelper.headers({"x-retry" => 5_i64})).should be_true
      end
    end

    describe "op: not_eq" do
      it "matches when header value differs" do
        c = LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::NotEq, "premium")
        c.match?(QueueFilterSpecHelper.headers({"x-tier" => "basic"})).should be_true
      end

      it "does not match when header equals" do
        c = LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::NotEq, "premium")
        c.match?(QueueFilterSpecHelper.headers({"x-tier" => "premium"})).should be_false
      end

      it "matches when header missing (absence is not-equal)" do
        c = LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::NotEq, "premium")
        c.match?(QueueFilterSpecHelper.headers({"other" => "x"})).should be_true
      end

      it "matches when headers nil" do
        c = LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::NotEq, "premium")
        c.match?(nil).should be_true
      end
    end

    describe "op: exists" do
      it "matches when key present regardless of value" do
        c = LavinMQ::QueueFilter::Clause.new("x-flag", LavinMQ::QueueFilter::Op::Exists)
        c.match?(QueueFilterSpecHelper.headers({"x-flag" => ""})).should be_true
        c.match?(QueueFilterSpecHelper.headers({"x-flag" => "anything"})).should be_true
      end

      it "does not match when key absent" do
        c = LavinMQ::QueueFilter::Clause.new("x-flag", LavinMQ::QueueFilter::Op::Exists)
        c.match?(QueueFilterSpecHelper.headers({"other" => "x"})).should be_false
      end

      it "does not match when headers nil" do
        c = LavinMQ::QueueFilter::Clause.new("x-flag", LavinMQ::QueueFilter::Op::Exists)
        c.match?(nil).should be_false
      end
    end
  end

  describe LavinMQ::QueueFilter::Rule do
    describe "match? with x_match all" do
      it "matches when all clauses match" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [
            LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::Eq, "free"),
            LavinMQ::QueueFilter::Clause.new("x-region", LavinMQ::QueueFilter::Op::Eq, "eu"),
          ],
          x_match: LavinMQ::QueueFilter::XMatch::All,
        )
        r.match?(QueueFilterSpecHelper.props({"x-tier" => "free", "x-region" => "eu"})).should be_true
      end

      it "does not match if one clause fails" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [
            LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::Eq, "free"),
            LavinMQ::QueueFilter::Clause.new("x-region", LavinMQ::QueueFilter::Op::Eq, "eu"),
          ],
          x_match: LavinMQ::QueueFilter::XMatch::All,
        )
        r.match?(QueueFilterSpecHelper.props({"x-tier" => "free", "x-region" => "us"})).should be_false
      end
    end

    describe "match? with x_match any" do
      it "matches when any clause matches" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [
            LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::Eq, "free"),
            LavinMQ::QueueFilter::Clause.new("x-region", LavinMQ::QueueFilter::Op::Eq, "eu"),
          ],
          x_match: LavinMQ::QueueFilter::XMatch::Any,
        )
        r.match?(QueueFilterSpecHelper.props({"x-tier" => "free", "x-region" => "us"})).should be_true
      end

      it "does not match if no clause matches" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [
            LavinMQ::QueueFilter::Clause.new("x-tier", LavinMQ::QueueFilter::Op::Eq, "free"),
            LavinMQ::QueueFilter::Clause.new("x-region", LavinMQ::QueueFilter::Op::Eq, "eu"),
          ],
          x_match: LavinMQ::QueueFilter::XMatch::Any,
        )
        r.match?(QueueFilterSpecHelper.props({"x-tier" => "paid", "x-region" => "us"})).should be_false
      end
    end

    describe "replay loop prevention" do
      it "never matches messages carrying x-source-queue header" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x-test", LavinMQ::QueueFilter::Op::Exists)],
        )
        replayed = QueueFilterSpecHelper.props({
          "x-test"         => "any",
          "x-source-queue" => "src-q",
        })
        r.match?(replayed).should be_false
      end
    end

    describe "validate!" do
      it "raises when clauses empty" do
        r = LavinMQ::QueueFilter::Rule.new(clauses: [] of LavinMQ::QueueFilter::Clause)
        expect_raises(LavinMQ::Error::PreconditionFailed, /at least one clause/) do
          r.validate!
        end
      end

      it "raises when move_to action has nil target" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x", LavinMQ::QueueFilter::Op::Exists)],
          action: LavinMQ::QueueFilter::Action::MoveTo,
          target: nil,
        )
        expect_raises(LavinMQ::Error::PreconditionFailed, /target/) do
          r.validate!
        end
      end

      it "raises when duplicate_to action has empty target" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x", LavinMQ::QueueFilter::Op::Exists)],
          action: LavinMQ::QueueFilter::Action::DuplicateTo,
          target: "",
        )
        expect_raises(LavinMQ::Error::PreconditionFailed, /target/) do
          r.validate!
        end
      end

      it "raises when eq clause missing value" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x", LavinMQ::QueueFilter::Op::Eq, nil)],
        )
        expect_raises(LavinMQ::Error::PreconditionFailed, /requires a value/) do
          r.validate!
        end
      end

      it "accepts drop action with no target" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x", LavinMQ::QueueFilter::Op::Exists)],
        )
        r.validate!
      end

      it "accepts exists clause with no value" do
        r = LavinMQ::QueueFilter::Rule.new(
          clauses: [LavinMQ::QueueFilter::Clause.new("x", LavinMQ::QueueFilter::Op::Exists)],
        )
        r.validate!
      end
    end

    describe ".parse" do
      it "parses a valid move_to rule" do
        json = %({
          "x-match": "all",
          "action": "move_to",
          "target": "audit-bin",
          "rule_id": "rule-1",
          "clauses": [
            {"key": "x-tier", "op": "eq", "value": "free"},
            {"key": "x-debug", "op": "exists"}
          ]
        })
        r = LavinMQ::QueueFilter::Rule.parse(json)
        r.x_match.should eq LavinMQ::QueueFilter::XMatch::All
        r.action.should eq LavinMQ::QueueFilter::Action::MoveTo
        r.target.should eq "audit-bin"
        r.rule_id.should eq "rule-1"
        r.clauses.size.should eq 2
      end

      it "defaults x-match to all and action to drop" do
        json = %({"clauses": [{"key": "x-bad", "op": "exists"}]})
        r = LavinMQ::QueueFilter::Rule.parse(json)
        r.x_match.should eq LavinMQ::QueueFilter::XMatch::All
        r.action.should eq LavinMQ::QueueFilter::Action::Drop
      end

      it "parses duplicate_to action with target" do
        json = %({
          "action": "duplicate_to",
          "target": "audit-q",
          "clauses": [{"key": "x-audit", "op": "exists"}]
        })
        r = LavinMQ::QueueFilter::Rule.parse(json)
        r.action.should eq LavinMQ::QueueFilter::Action::DuplicateTo
        r.target.should eq "audit-q"
      end

      it "raises on move_to without target" do
        json = %({
          "action": "move_to",
          "clauses": [{"key": "x-audit", "op": "exists"}]
        })
        expect_raises(LavinMQ::Error::PreconditionFailed, /target/) do
          LavinMQ::QueueFilter::Rule.parse(json)
        end
      end

      it "raises on empty clauses" do
        json = %({"clauses": []})
        expect_raises(LavinMQ::Error::PreconditionFailed, /at least one clause/) do
          LavinMQ::QueueFilter::Rule.parse(json)
        end
      end
    end
  end
end
