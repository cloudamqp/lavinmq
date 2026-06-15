require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/membership"

alias Membership = LavinMQ::Clustering::VR::Membership

describe LavinMQ::Clustering::VR::Membership do
  describe ".parse" do
    it "parses an id=uri roster and locates this node" do
      m = Membership.parse("1=tcp://n1:5679,2=tcp://n2:5679,3=tcp://n3:5679", "tcp://n2:5679")
      m.size.should eq 3
      m.self_id.should eq 2
      m.self_member.uri.should eq "tcp://n2:5679"
      m.members.map(&.id).should eq [1, 2, 3]
    end

    it "sorts members by id regardless of roster order" do
      m = Membership.parse("3=tcp://n3:5679,1=tcp://n1:5679,2=tcp://n2:5679", "tcp://n1:5679")
      m.members.map(&.id).should eq [1, 2, 3]
    end

    it "tolerates whitespace and trailing commas" do
      m = Membership.parse(" 1=tcp://n1:5679 , 2=tcp://n2:5679 ,", "tcp://n1:5679")
      m.size.should eq 2
    end

    it "parses ids as base-36" do
      m = Membership.parse("z=tcp://n1:5679,10=tcp://n2:5679", "tcp://n1:5679")
      m.includes?(35).should be_true # "z" base-36
      m.includes?(36).should be_true # "10" base-36
    end

    it "raises on a malformed pair" do
      expect_raises(LavinMQ::Clustering::VR::Error, /Malformed/) do
        Membership.parse("1=tcp://n1:5679,bogus", "tcp://n1:5679")
      end
    end

    it "raises on a non-base-36 id" do
      expect_raises(LavinMQ::Clustering::VR::Error, /Invalid clustering member id/) do
        Membership.parse("!=tcp://n1:5679", "tcp://n1:5679")
      end
    end

    it "raises on duplicate ids" do
      expect_raises(LavinMQ::Clustering::VR::Error, /Duplicate clustering member id/) do
        Membership.parse("1=tcp://n1:5679,1=tcp://n2:5679", "tcp://n1:5679")
      end
    end

    it "raises on duplicate uris" do
      expect_raises(LavinMQ::Clustering::VR::Error, /Duplicate clustering member uri/) do
        Membership.parse("1=tcp://n1:5679,2=tcp://n1:5679", "tcp://n1:5679")
      end
    end

    it "raises on an empty roster" do
      expect_raises(LavinMQ::Clustering::VR::Error, /No clustering members/) do
        Membership.parse("", "tcp://n1:5679")
      end
    end

    it "raises when this node is not in the roster" do
      expect_raises(LavinMQ::Clustering::VR::Error, /not in the clustering member roster/) do
        Membership.parse("1=tcp://n1:5679,2=tcp://n2:5679", "tcp://n9:5679")
      end
    end
  end

  describe "#quorum" do
    it "is a strict majority" do
      Membership.parse("1=tcp://a:1", "tcp://a:1").quorum.should eq 1
      Membership.parse("1=tcp://a:1,2=tcp://b:1", "tcp://a:1").quorum.should eq 2
      Membership.parse("1=tcp://a:1,2=tcp://b:1,3=tcp://c:1", "tcp://a:1").quorum.should eq 2
      Membership.parse("1=tcp://a:1,2=tcp://b:1,3=tcp://c:1,4=tcp://d:1,5=tcp://e:1", "tcp://a:1").quorum.should eq 3
    end
  end

  describe "#primary_of" do
    it "is deterministic round-robin over the sorted roster" do
      m = Membership.parse("3=tcp://c:1,1=tcp://a:1,2=tcp://b:1", "tcp://a:1")
      m.primary_of(0u64).id.should eq 1
      m.primary_of(1u64).id.should eq 2
      m.primary_of(2u64).id.should eq 3
      m.primary_of(3u64).id.should eq 1
    end
  end

  describe "#peers" do
    it "excludes this node" do
      m = Membership.parse("1=tcp://a:1,2=tcp://b:1,3=tcp://c:1", "tcp://b:1")
      m.peers.map(&.id).should eq [1, 3]
    end
  end

  describe "#uri_for" do
    it "returns the uri for a member id, nil otherwise" do
      m = Membership.parse("1=tcp://a:1,2=tcp://b:1", "tcp://a:1")
      m.uri_for(2).should eq "tcp://b:1"
      m.uri_for(99).should be_nil
    end
  end

  describe ".committed_op" do
    it "is the quorum-th largest applied op" do
      LavinMQ::Clustering::VR::Membership.committed_op(2, [10u64, 5u64, 3u64]).should eq 5
      LavinMQ::Clustering::VR::Membership.committed_op(3, [10u64, 9u64, 8u64, 7u64, 6u64]).should eq 8
    end

    it "returns nil when fewer than quorum members are present" do
      LavinMQ::Clustering::VR::Membership.committed_op(2, [10u64]).should be_nil
      LavinMQ::Clustering::VR::Membership.committed_op(3, [10u64, 5u64]).should be_nil
    end

    it "handles a single-member quorum and ties" do
      LavinMQ::Clustering::VR::Membership.committed_op(1, [7u64]).should eq 7
      LavinMQ::Clustering::VR::Membership.committed_op(2, [5u64, 5u64]).should eq 5
    end
  end
end
