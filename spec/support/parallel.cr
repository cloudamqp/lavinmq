require "spec"
require "json"
require "file_utils"

lib LibC
  fun dup(fd : Int) : Int
end

# With SPEC_JOBS above 1, runs the examples in that many workers of this process and reports them in order
module ParallelSpec
  JOBS   = ENV["SPEC_JOBS"]?.try(&.to_i) || 1
  WORKER = ENV["SPEC_WORKER"]?.try(&.to_i)

  # The arguments this process was started with, as the spec runner consumes ARGV
  def self.argv : Array(String)
    (1...ARGC_UNSAFE).map { |i| String.new(ARGV_UNSAFE[i]) }
  end

  def self.parallel? : Bool
    WORKER.nil? && JOBS > 1
  end

  class_getter! worker : Worker
  class_getter! coordinator : Coordinator

  class Worker
    @index = -1i64
    @ticket = -1i64
    @queue : File
    @log : String
    @events : IO::FileDescriptor

    def initialize
      @queue = File.open(ENV["SPEC_QUEUE"], "a")
      @log = ENV["SPEC_LOG"]
      # Results go to the coordinator on the original stdout, everything the examples print to the log
      @events = IO::FileDescriptor.new(LibC.dup(STDOUT.fd))
      @events.close_on_exec = true
      log = File.open(@log, "a")
      STDOUT.reopen(log)
      STDERR.reopen(log)
      log.close
    end

    # Claims the next example in the shared order, if no other worker has
    def claim? : Bool
      @index += 1
      @ticket = next_ticket if @ticket < @index
      return false unless @ticket == @index
      event({run: @index})
      true
    end

    def report(result : Spec::Result) : Nil
      ex = result.exception
      event({
        done:     @index,
        status:   result.kind.to_s,
        elapsed:  result.elapsed.try(&.total_seconds),
        output:   output_size,
        message:  ex.try { |e| e.is_a?(Spec::SpecError) ? e.to_s : e.inspect_with_backtrace }.try(&.scrub),
        spec_err: ex.is_a?(Spec::SpecError),
        file:     ex.as?(Spec::SpecError).try(&.file),
        line:     ex.as?(Spec::SpecError).try(&.line),
      })
    end

    private def output_size : Int64
      STDOUT.flush
      STDERR.flush
      File.size(@log).to_i64
    end

    private def event(data) : Nil
      @events.puts data.to_json
      @events.flush
    end

    # The counter is the queue file's size: each claim appends one byte
    private def next_ticket : Int64
      @queue.flock_exclusive do
        ticket = @queue.size.to_i64
        @queue.write_byte(0u8)
        @queue.flush
        ticket
      end
    end
  end

  class Formatter < Spec::Formatter
    def report(result)
      ParallelSpec.worker.report(result)
    end
  end

  # An exception from a worker, printed as the worker printed it
  class RemoteException < Exception
    def initialize(@text : String)
      super(@text.lines.first? || "")
    end

    def inspect_with_backtrace(io : IO) : Nil
      io << @text
    end
  end

  class Coordinator
    record Run, worker : Int32, index : Int64
    record Done, worker : Int32, index : Int64, status : Spec::Status, elapsed : Time::Span?,
      exception : Exception?, output : Int64
    record Exited, worker : Int32, status : Process::Status, stray : String

    @events = Channel(Run | Done | Exited).new(256)
    @ready = Deque(Done).new
    @running = Hash(Int32, Int64).new
    @printed = Hash(Int32, Int64).new(0i64)
    @exited = Hash(Int32, Exited).new
    @processes = Array(Process).new
    @dir : String

    def initialize(@cli : Spec::CLI)
      @dir = File.tempname("lavinmq", "specs")
      Dir.mkdir_p @dir
      File.touch queue_path
      at_exit { stop }
    end

    def start : Nil
      executable = Process.executable_path || raise "Can't find this executable to start workers of"
      args = worker_args
      JOBS.times do |i|
        env = {"SPEC_WORKER" => i.to_s, "SPEC_WORKERS" => JOBS.to_s, "SPEC_QUEUE" => queue_path, "SPEC_LOG" => log_path(i)}
        process = File.open(log_path(i), "a") do |log|
          Process.new(executable, args, env: env, output: Process::Redirect::Pipe, error: log)
        end
        @processes << process
        spawn(name: "spec worker #{i}") { read_events(i, process) }
      end
    end

    # Reports each example as soon as a worker has run it, under the headers of the groups it is in
    def report_all(root : Spec::RootContext) : Nil
      examples = [] of Spec::Example
      collect(root, examples)
      groups = [] of Spec::ExampleGroup
      examples.size.times do |reported|
        done = next_done(examples.size - reported)
        example = examples[done.index]
        path = ancestors(example)
        common = groups.zip?(path).index { |(group, other)| !group.same?(other) } || {groups.size, path.size}.min
        (groups.size - common).times do
          groups.pop
          @cli.formatters.each(&.pop)
        end
        path[common..].each do |group|
          groups << group
          @cli.formatters.each(&.push(group))
        end
        @cli.formatters.each(&.before_example(example.description))
        print_output(done.worker, done.output)
        example.report_done(done.status, done.elapsed, done.exception)
        @cli.abort! if @cli.fail_fast? && done.status.in?(Spec::Status::Fail, Spec::Status::Error)
      end
      groups.size.times { @cli.formatters.each(&.pop) }
    end

    private def next_done(left : Int32) : Done
      while @ready.empty?
        raise "#{left} examples were not run, see the output of the workers above" if @exited.size == JOBS
        handle @events.receive
      end
      @ready.shift
    end

    private def collect(context : Spec::Context, examples : Array(Spec::Example)) : Nil
      context.children.each do |child|
        child.is_a?(Spec::Example) ? examples << child : collect(child, examples)
      end
    end

    private def ancestors(example : Spec::Example) : Array(Spec::ExampleGroup)
      path = [] of Spec::ExampleGroup
      context = example.parent
      while context.is_a?(Spec::ExampleGroup)
        path.unshift context
        context = context.parent
      end
      path
    end

    # Lets the workers exit, then prints anything they printed after their last example
    def finish(aborted : Bool) : Nil
      return stop if aborted
      deadline = Time.instant + 10.seconds
      while @exited.size < JOBS
        select
        when event = @events.receive
          handle event
        when timeout(deadline - Time.instant)
          break
        end
      end
      JOBS.times do |i|
        print_output(i, File.size(log_path(i)).to_i64) if File.exists?(log_path(i))
        if exited = @exited[i]?
          @cli.stdout.print exited.stray
        end
      end
    end

    private def handle(event : Run) : Nil
      @running[event.worker] = event.index
    end

    private def handle(event : Done) : Nil
      @running.delete(event.worker)
      @ready << event
    end

    private def handle(event : Exited) : Nil
      @exited[event.worker] = event
      if index = @running.delete(event.worker)
        ex = RemoteException.new("Worker #{event.worker} exited (#{event.status}) while running this example, its output is above")
        @ready << Done.new(event.worker, index, :error, nil, ex, File.size(log_path(event.worker)).to_i64)
      end
    end

    private def read_events(worker : Int32, process : Process) : Nil
      stray = String::Builder.new
      process.output.each_line do |line|
        if line.starts_with?('{')
          @events.send parse(worker, JSON.parse(line))
        else
          stray << line << '\n'
        end
      end
      status = process.wait
      @events.send Exited.new(worker, status, stray.to_s)
    end

    private def parse(worker, json) : Run | Done
      if index = json["run"]?
        return Run.new(worker, index.as_i64)
      end
      message = json["message"].as_s?
      ex = if message.nil?
             nil
           elsif json["spec_err"].as_bool
             Spec::AssertionFailed.new(message, json["file"].as_s, json["line"].as_i)
           else
             RemoteException.new(message)
           end
      Done.new(worker, json["done"].as_i64, Spec::Status.parse(json["status"].as_s),
        json["elapsed"].as_f?.try(&.seconds), ex, json["output"].as_i64)
    end

    # Prints what the worker printed up to *offset*, so an example's output shows up with it
    private def print_output(worker : Int32, offset : Int64) : Nil
      from = @printed[worker]
      return if offset <= from
      File.open(log_path(worker)) do |f|
        f.seek(from)
        IO.copy(f, @cli.stdout, offset - from)
      end
      @printed[worker] = offset
    end

    private def worker_args : Array(String)
      args = [] of String
      skip = false
      ParallelSpec.argv.each do |arg|
        if skip
          skip = false
        elsif arg == "--order"
          skip = true
        elsif !arg.starts_with?("--order=")
          args << arg
        end
      end
      if seed = @cli.randomizer_seed
        args << "--order" << seed.to_s
      end
      args
    end

    private def queue_path
      File.join(@dir, "queue")
    end

    private def log_path(worker)
      File.join(@dir, "#{worker}.log")
    end

    private def stop : Nil
      @processes.each_with_index { |p, i| p.terminate unless @exited.has_key?(i) rescue nil }
      FileUtils.rm_rf @dir
    end
  end

  def self.start(cli : Spec::CLI) : Nil
    if WORKER
      @@worker = Worker.new
      cli.formatters.clear
      cli.add_formatter Formatter.new(cli)
    elsif parallel? && !cli.dry_run?
      @@coordinator = Coordinator.new(cli).tap(&.start)
    end
  end
end

class Spec::CLI
  def finish_run
    if ParallelSpec.worker?
      root_context.finish(@start_time.try(&.elapsed) || Time::Span.zero, @aborted)
      Process.exit(1) if @aborted
    else
      previous_def
    end
  end
end

class Spec::RootContext
  def print_order_message(io : IO)
    previous_def unless ParallelSpec.worker?
  end

  def run
    ParallelSpec.start(cli)
    if coordinator = ParallelSpec.coordinator?
      print_order_message(cli.stdout)
      run_before_all_hooks
      coordinator.report_all(self)
      run_after_all_hooks
    else
      previous_def
    end
  end

  def finish(elapsed_time, aborted = false)
    ParallelSpec.coordinator?.try &.finish(aborted)
    previous_def
  end
end

class Spec::Example
  def run
    if worker = ParallelSpec.worker?
      previous_def if worker.claim?
    else
      previous_def
    end
  end

  # Reports a result a worker sent for this example
  def report_done(status : Spec::Status, elapsed : Time::Span?, exception : Exception?) : Nil
    @parent.report(status, description, file, line, elapsed, exception)
  end
end
