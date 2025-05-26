require "../spec/spec_helper.cr"

def setup_running_shovel(amqp_url, shovel_store, name = %{#{__FILE__}-test-upsert})
    create_payload = JSON.parse(%{
      {
        "src-uri":"#{amqp_url}",
        "dest-uri":"#{amqp_url}",
        "src-prefetch-count":1000,
        "src-delete-after":"never",
        "reconnect-delay":5,
        "ack-mode":"on-confirm",
        "src-queue":"q1",
        "dest-queue":"q2"
      }
    })
  shovel_store.upsert(name, create_payload)

  running_shovel = shovel_store[name]

  should_eventually(eq LavinMQ::Shovel::State::Running) {
    running_shovel.state
  }

  running_shovel
end

def setup_paused_shovel(amqp_url, shovel_store, name = %{#{__FILE__}-test-upsert})
  shovel = setup_running_shovel(amqp_url, shovel_store, name)
  pause_payload = JSON.parse(%{{ "state": "Paused" }})
  shovel_store.upsert(name, pause_payload)
  running_shovel = shovel_store[name]
  should_eventually(eq LavinMQ::Shovel::State::Paused) { running_shovel.state }
end

describe LavinMQ::ShovelStore do
  describe "upsert" do

    it "can pause" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.shovels.size.should eq 0
        setup_paused_shovel(s.amqp_url, vhost.shovels)
      end
    end

    it "can resume" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.shovels.size.should eq 0
        shovel_name = "test-resume"
        setup_paused_shovel(s.amqp_url, vhost.shovels, shovel_name)

        vhost.shovels.upsert(shovel_name, JSON.parse(%{
          {
            "state": "Running"
          }
        }))

        running_shovel = vhost.shovels[shovel_name]
        should_eventually(eq LavinMQ::Shovel::State::Running) { running_shovel.state }
      end
    end
  end
end
