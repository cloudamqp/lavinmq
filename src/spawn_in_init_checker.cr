# Require compiler components individually to skip interpreter.cr (needs
# `reply` shard) and command.cr (needs doc tools which need `markd`).
require "compiler/crystal/annotatable"
require "compiler/crystal/program"
require "compiler/crystal/compiler"
require "compiler/crystal/config"
require "compiler/crystal/crystal_path"
require "compiler/crystal/error"
require "compiler/crystal/exception"
require "compiler/crystal/formatter"
require "compiler/crystal/loader"
require "compiler/crystal/macros"
require "compiler/crystal/progress_tracker"
require "compiler/crystal/semantic"
require "compiler/crystal/syntax"
require "compiler/crystal/types"
require "compiler/crystal/util"
require "compiler/crystal/warnings"
require "compiler/crystal/semantic/*"
require "compiler/crystal/macros/*"
require "compiler/crystal/codegen/*"
require "compiler/crystal/tools/dependencies"

# Transitive spawn-in-initialize detector.
#
# Uses the Crystal compiler's semantic analysis to find `initialize` methods
# that can transitively reach a `spawn` call. This catches cases that the
# simple Ameba AST rule misses (e.g., initialize -> start_loop -> spawn).
#
# Approach:
# 1. Run semantic analysis (no codegen) to get a fully typed AST
# 2. Walk the reachable AST from result.node, following Call#target_defs
#    to collect the TYPED versions of all reachable defs (template defs
#    from type.defs don't have target_defs populated)
# 3. For each typed initialize def in project code, find ALL paths to
#    `spawn` via DFS through resolved Call#target_defs
# 4. Report all chains grouped by initialize, shortest first
module SpawnInInitChecker
  record Violation, init_def : Crystal::Def, chains : Array(Array(String)), created_by : Array(String)

  # Walk the reachable AST to collect typed defs. After semantic analysis,
  # only defs reachable through Call#target_defs have their inner calls
  # resolved. This mirrors how Crystal's unreachable tool works.
  class ReachableDefCollector < Crystal::Visitor
    getter typed_defs = Set(Crystal::Def).new.compare_by_identity
    # Maps each def to the set of defs that call it
    getter callers = Hash(Crystal::Def, Set(Crystal::Def)).new.compare_by_identity
    @visited = Set(Crystal::ASTNode).new.compare_by_identity
    @def_stack = [] of Crystal::Def

    def visit(node : Crystal::Call)
      if expanded = node.expanded
        expanded.accept(self)
        return true
      end

      node.target_defs.try &.each do |a_def|
        # Always record the caller relationship (even if already visited)
        if caller = @def_stack.last?
          (@callers[a_def] ||= Set(Crystal::Def).new.compare_by_identity).add(caller)
        end

        if @visited.add?(a_def)
          @typed_defs.add(a_def)
          @def_stack.push(a_def)
          a_def.body.accept(self)
          @def_stack.pop
        end
      end

      true
    end

    def visit(node : Crystal::ExpandableNode)
      return false unless @visited.add?(node)

      if expanded = node.expanded
        expanded.accept(self)
      end

      true
    end

    def visit(node : Crystal::ClassDef)
      node.resolved_type.instance_vars_initializers.try &.each do |initializer|
        initializer.value.accept(self)
      end

      true
    end

    def visit(node)
      true
    end
  end

  # Find all spawn chains per initialize via memoized DFS.
  class Checker
    # Memoize: for each def, does it reach spawn? Store the shortest chain.
    @spawn_cache = Hash(Crystal::Def, Array(String)?).new.compare_by_identity
    @on_stack = Set(Crystal::Def).new.compare_by_identity
    @callers = Hash(Crystal::Def, Set(Crystal::Def)).new.compare_by_identity
    @project_path : String

    def initialize(@project_path : String)
    end

    def run(program : Crystal::Program, node : Crystal::ASTNode) : Array(Violation)
      collector = ReachableDefCollector.new
      node.accept(collector)
      @callers = collector.callers

      init_defs = collector.typed_defs.select do |d|
        d.name == "initialize" && in_project?(d.location)
      end

      violations = [] of Violation
      init_defs.each do |init_def|
        chains = find_all_spawn_chains(init_def)
        unless chains.empty?
          created_by = find_construction_sites(init_def)
          violations << Violation.new(init_def, chains.sort_by(&.size), created_by)
        end
      end

      # Deduplicate by location, merging chains
      by_location = Hash(String, Violation).new
      violations.each do |v|
        loc = v.init_def.location.not_nil!
        key = "#{loc.filename}:#{loc.line_number}"
        if existing = by_location[key]?
          # Merge chains, dedup by string representation
          seen = existing.chains.map(&.join(" -> ")).to_set
          v.chains.each do |chain|
            repr = chain.join(" -> ")
            unless seen.includes?(repr)
              existing.chains << chain
              seen.add(repr)
            end
          end
          existing.chains.sort_by!(&.size)
          # Merge created_by sites
          existing_sites = existing.created_by.to_set
          v.created_by.each do |site|
            unless existing_sites.includes?(site)
              existing.created_by << site
              existing_sites.add(site)
            end
          end
        else
          by_location[key] = v
        end
      end

      by_location.values.sort_by! do |v|
        loc = v.init_def.location.not_nil!
        {loc.filename.as(String), loc.line_number, loc.column_number}
      end
    end

    # Find ALL distinct spawn chains in a def body.
    # Each chain represents a unique first-hop path to spawn.
    private def find_all_spawn_chains(a_def : Crystal::Def) : Array(Array(String))
      return [] of Array(String) if @on_stack.includes?(a_def)

      @on_stack.add(a_def)
      scanner = AllChainsScanner.new(self)
      a_def.body.accept(scanner)
      @on_stack.delete(a_def)
      scanner.chains
    end

    # Check if a callee def can reach spawn (memoized, returns shortest chain).
    def callee_spawn_chain(a_def : Crystal::Def) : Array(String)?
      if @spawn_cache.has_key?(a_def)
        return @spawn_cache[a_def]
      end

      return nil if @on_stack.includes?(a_def)

      @on_stack.add(a_def)
      scanner = FirstChainScanner.new(self)
      a_def.body.accept(scanner)
      result = scanner.result
      @on_stack.delete(a_def)
      @spawn_cache[a_def] = result
      result
    end

    # Trace back from an initialize def to find where the object is constructed.
    # Path: initialize <- new <- caller(s) of new
    private def find_construction_sites(init_def : Crystal::Def) : Array(String)
      sites = [] of String

      # Find `new` methods that call this initialize
      new_defs = @callers[init_def]?
      return sites unless new_defs

      new_defs.each do |new_def|
        next unless new_def.name == "new"

        # Find callers of the `new` method
        new_callers = @callers[new_def]?
        next unless new_callers

        new_callers.each do |caller_def|
          loc = caller_def.location
          next unless loc
          filename = loc.filename
          next unless filename.is_a?(String)
          sites << "#{caller_def.short_reference} (#{Path[filename].relative_to(@project_path)}:#{loc.line_number})"
        end
      end

      sites.sort!
      sites.uniq!
      sites
    end

    def in_project?(location : Crystal::Location?) : Bool
      return false unless location
      filename = location.filename
      return false unless filename.is_a?(String)
      filename.starts_with?(@project_path) && !filename.starts_with?(File.join(@project_path, "lib/"))
    end
  end

  # Collects ALL distinct spawn chains from a def body.
  # Does NOT stop at the first spawn — continues to find every path.
  class AllChainsScanner < Crystal::Visitor
    getter chains = [] of Array(String)
    @visited = Set(Crystal::ASTNode).new.compare_by_identity

    def initialize(@checker : Checker)
    end

    def visit(node : Crystal::Call)
      if expanded = node.expanded
        expanded.accept(self)
      end

      if node.name == "spawn"
        @chains << ["spawn"]
        return true # keep going to find more
      end

      node.target_defs.try &.each do |a_def|
        next unless @visited.add?(a_def)

        if chain = @checker.callee_spawn_chain(a_def)
          @chains << [a_def.short_reference] + chain
        end
      end

      true
    end

    def visit(node : Crystal::ExpandableNode)
      return false unless @visited.add?(node)

      if expanded = node.expanded
        expanded.accept(self)
      end

      true
    end

    def visit(node : Crystal::ClassDef | Crystal::ModuleDef | Crystal::EnumDef)
      false
    end

    def visit(node)
      true
    end
  end

  # Finds the FIRST (shortest) spawn chain — used for memoized callee checks.
  class FirstChainScanner < Crystal::Visitor
    getter result : Array(String)?
    @visited = Set(Crystal::ASTNode).new.compare_by_identity

    def initialize(@checker : Checker)
      @result = nil
    end

    def visit(node : Crystal::Call)
      return false if @result

      if expanded = node.expanded
        expanded.accept(self)
        return false if @result
      end

      if node.name == "spawn"
        @result = ["spawn"]
        return false
      end

      node.target_defs.try &.each do |a_def|
        break if @result
        next unless @visited.add?(a_def)

        if chain = @checker.callee_spawn_chain(a_def)
          @result = [a_def.short_reference] + chain
          return false
        end
      end

      true
    end

    def visit(node : Crystal::ExpandableNode)
      return false if @result
      return false unless @visited.add?(node)

      if expanded = node.expanded
        expanded.accept(self)
      end

      true
    end

    def visit(node : Crystal::ClassDef | Crystal::ModuleDef | Crystal::EnumDef)
      false
    end

    def visit(node)
      @result.nil?
    end
  end
end

project_path = File.expand_path(File.join(__DIR__, ".."))

save_baseline : String? = nil
check_baseline : String? = nil
changed_files_path : String? = nil
source_file : String? = nil

args = ARGV.dup
i = 0
while i < args.size
  case args[i]
  when "--save-baseline"
    save_baseline = args[i + 1]?
    abort "Missing filename for --save-baseline" unless save_baseline
    i += 2
  when "--check-baseline"
    check_baseline = args[i + 1]?
    abort "Missing filename for --check-baseline" unless check_baseline
    i += 2
  when "--changed-files"
    changed_files_path = args[i + 1]?
    abort "Missing filename for --changed-files" unless changed_files_path
    i += 2
  else
    source_file = args[i]
    i += 1
  end
end

source_file ||= File.join(project_path, "src", "lavinmq.cr")

unless File.exists?(source_file)
  STDERR.puts "Source file not found: #{source_file}"
  exit 1
end

# Our binary doesn't have Crystal::Config.path set (that's only baked into
# the compiler build), so the embedded compiler can't find the prelude.
# Resolve it the same way `crystal env CRYSTAL_PATH` does.
unless ENV["CRYSTAL_PATH"]?
  crystal = Process.find_executable("crystal")
  if crystal
    origin = File.dirname(File.realpath(crystal))
    ENV["CRYSTAL_PATH"] = "lib:#{File.expand_path("../share/crystal/src", origin)}"
  end
end

compiler = Crystal::Compiler.new
compiler.no_codegen = true
compiler.flags << "preview_mt"
compiler.flags << "execution_context"

source = Crystal::Compiler::Source.new(source_file, File.read(source_file))
result = compiler.compile(source, "")

checker = SpawnInInitChecker::Checker.new(project_path)
violations = checker.run(result.program, result.node)
current_dir = Dir.current

# Build a map of short_reference -> Violation (stable across line number changes)
violation_map = Hash(String, SpawnInInitChecker::Violation).new
violations.each do |v|
  violation_map[v.init_def.short_reference] = v
end

if baseline_path = save_baseline
  File.open(baseline_path, "w") do |f|
    f.puts "# spawn-in-init-checker baseline"
    f.puts "# Each entry is a type#initialize that is known to transitively reach spawn."
    f.puts "# Remove entries as you refactor each one (move spawn to a #start method)."
    violations.each do |v|
      f.puts v.init_def.short_reference
    end
  end
  STDERR.puts "Saved baseline with #{violations.size} entries to #{baseline_path}"
  exit 0
end

if baseline_path = check_baseline
  baseline_refs = Set(String).new
  File.each_line(baseline_path) do |line|
    line = line.strip
    next if line.empty? || line.starts_with?('#')
    baseline_refs.add(line)
  end

  new_violations = [] of SpawnInInitChecker::Violation
  dropped_refs = [] of String
  pending_count = 0

  # Classify each baseline entry as PENDING or DROPPED
  baseline_refs.each do |ref|
    if violation_map.has_key?(ref)
      pending_count += 1
    else
      dropped_refs << ref
    end
  end

  # Find NEW violations not in baseline
  violations.each do |v|
    unless baseline_refs.includes?(v.init_def.short_reference)
      new_violations << v
    end
  end

  # Output results
  unless new_violations.empty?
    puts "NEW spawn-in-initialize violations:"
    puts
    new_violations.each do |v|
      loc = v.init_def.location.not_nil!
      filename = Path[loc.filename.as(String)].relative_to(current_dir)
      puts "  #{v.init_def.short_reference} (#{filename}:#{loc.line_number})"
      v.chains.first(3).each do |chain|
        puts "    Chain: #{v.init_def.short_reference} -> #{chain.join(" -> ")}"
      end
      unless v.created_by.empty?
        v.created_by.each do |site|
          puts "    Created by: #{site}"
        end
      end
      puts
    end
  end

  unless dropped_refs.empty?
    puts "FIXED spawn-in-initialize violations (remove from baseline):"
    puts
    dropped_refs.each do |ref|
      puts "  #{ref}"
    end
    puts
  end

  STDERR.puts "Baseline check: #{pending_count} pending, #{dropped_refs.size} fixed, #{new_violations.size} new"

  # Only fail on new violations
  exit new_violations.empty? ? 0 : 1
end

# Filter to changed files if specified
if cfp = changed_files_path
  changed_files = Set(String).new
  File.each_line(cfp) do |line|
    line = line.strip
    next if line.empty?
    changed_files.add(File.expand_path(line, current_dir))
  end

  filtered = violations.select do |v|
    loc = v.init_def.location.not_nil!
    changed_files.includes?(loc.filename.as(String))
  end

  if filtered.empty?
    puts "No spawn-in-initialize violations in changed files."
    exit 0
  end

  puts "NEW spawn-in-initialize violations in changed files:"
  puts
  filtered.each do |v|
    loc = v.init_def.location.not_nil!
    filename = Path[loc.filename.as(String)].relative_to(current_dir)
    puts "  #{v.init_def.short_reference} (#{filename}:#{loc.line_number})"
    v.chains.first(3).each do |chain|
      puts "    Chain: #{v.init_def.short_reference} -> #{chain.join(" -> ")}"
    end
    unless v.created_by.empty?
      v.created_by.each do |site|
        puts "    Created by: #{site}"
      end
    end
    puts
  end

  STDERR.puts "Found #{filtered.size} violation(s) in changed files."
  exit 1
end

# Default mode: just report all violations
if violations.empty?
  puts "No spawn-in-initialize violations found."
  exit 0
end

total_chains = 0
violations.each do |v|
  loc = v.init_def.location.not_nil!
  filename = Path[loc.filename.as(String)].relative_to(current_dir)
  puts "#{filename}:#{loc.line_number}"
  v.chains.each do |chain|
    puts "  Chain: #{v.init_def.short_reference} -> #{chain.join(" -> ")}"
    total_chains += 1
  end
  unless v.created_by.empty?
    v.created_by.each do |site|
      puts "  Created by: #{site}"
    end
  end
  puts
end

STDERR.puts "Found #{violations.size} initialize method(s) with #{total_chains} spawn chain(s)."
exit 1
