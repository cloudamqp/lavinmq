module LavinMQ
  module Utils
    # Create an alias for one or more named tuples. All given tuples will be merge to one.
    #
    # ```
    # alias_merged_tuple MyMergedTuple, NameTuple(a: Int332), NamedTuple(b: String)
    # ```
    # will generate
    #
    # ```
    # alias MyMergedTuple = NamedTuple(a: Int32, b: String)
    # ````
    #
    # Arguments can also be another alias:
    #
    # ```
    # alias MyTuple = NamedTuple(a: Int32)
    # alias OtherTuple = NamedTuple(b: String)
    # alias_merged_tuple MyMergedTuple, MyTuple, OtherTuple
    # ```
    # will generate
    # ```
    # alias MyMergedTuple = NamedTuple(a: Int32, b: String)
    # ```:w
    macro alias_merged_tuple(name, *tuples)
      {%
        raise "One or more NamedTuples is required" if tuples.size < 1
        members = [] of Nil
        tuples.each do |tuple|
          # if it's a generic we can assume a NamedTuple was pass directly
          if tuple.is_a?(Generic)
            members << tuple.named_args.double_splat
          else
            # tuple is an alias for a NamedTuple
            resolved = tuple.resolve
            resolved.instance.keys.each do |key|
              members << "#{key}: #{resolved[key]}"
            end
          end
        end
      %}
      alias {{name.id}} = NamedTuple(
        {{members.join(",").id}}
      )
    end
  end
end
