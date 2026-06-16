module LavinMQ
  module Clustering
    module LeaderHooks
      Log = LavinMQ::Log.for "clustering.leader_hooks"

      private def run_leader_elected_hook : Nil
        execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
      end

      private def run_leader_lost_hook : Nil
        execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
      end

      private def execute_shell_command(command : String, event : String) : Nil
        return if command.empty?

        Log.info { "Executing #{event} hook in background: #{command}" }
        spawn name: "#{event} hook" do
          begin
            status = Process.run(command, shell: true, output: Process::Redirect::Inherit, error: Process::Redirect::Inherit)
            if status.success?
              Log.info { "#{event} hook completed successfully" }
            else
              Log.warn { "#{event} hook failed with exit code #{status.exit_code}" }
            end
          rescue ex
            Log.error(exception: ex) { "Failed to execute #{event} hook" }
          end
        end
      end
    end
  end
end
