require "perf_tools/mem_prof"

module PerfTools::MemProf
  def self.log_objects_linked_to_type(io : IO, type : T.class, mermaid = false) : Nil forall T
    GC.collect
    stopping do
      type_id = T.crystal_instance_type_id
      alloc_infos = self.alloc_infos
      references = alloc_infos.select do |_, info|
        info.type_id == type_id
      end
      pointers = references.keys

      referees = Array({UInt64, UInt64}).new

      visited = [] of {UInt64, UInt64}

      alloc_infos.each do |ptr, info|
        next if info.type_id == 0 # skip allocations with no type info

        next if info.atomic

        next if visited.any? { |addr, _| addr == ptr }

        stack = [{ptr, info.size}]

        until stack.empty?
          subptr, size = stack.pop
          visited << {subptr, size}

          each_inner_pointer(Pointer(Void*).new(subptr), size) do |subptr|
            next unless subinfo = alloc_infos[subptr.address]?
            if pointers.includes? subptr.address
              tuple = {ptr, subptr.address}
              unless referees.includes? tuple
                pointers << ptr
                referees << tuple
              end
            elsif !subinfo.atomic && !visited.any? { |addr, size| addr == subptr.address }
              stack << {subptr.address, subinfo.size}
            end
          end
        end
      end

      if mermaid
        io << "graph LR\n"
      else
        io << referees.size << '\n'
      end

      referees.each do |ref|
        info = alloc_infos[ref[0]]
        name = known_classes[info.type_id]? || "(class #{info.type_id})"

        if mermaid
          io << "  " << ref[0] << "[\"" << ref[0] << " " << name << "\"] --> " << ref[1] << "\n"
        else
          io << ref[0] << '\t' << name << "\t" << ref[1] << '\n'
        end
      end
    end
  end
end
