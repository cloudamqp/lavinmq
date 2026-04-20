# Pure-Crystal policy pattern matcher — no libpcre2 dependency.
# Used by the WASM build; importable anywhere a Regex-free match is needed.
# Supports: ^ $ anchors, .* wildcard, \. escaped dot, [x] single-char classes.
module LavinMQ
  module PolicyPattern
    def self.matches?(pattern : String, subject : String) : Bool
      pat = expand_char_classes(pattern)
      pat = pat.gsub("\\.", "\u0000DOT\u0000")
      anchored_start = pat.starts_with?('^')
      anchored_end = pat.ends_with?('$') && !pat.ends_with?("\\$")
      core = pat.lchop('^').rchop('$')
      core = core.gsub(".*", "\u0000STAR\u0000")
      core = core.gsub("\u0000DOT\u0000", ".")
      match_parts(core.split("\u0000STAR\u0000"), subject, anchored_start, anchored_end)
    end

    private def self.expand_char_classes(pat : String) : String
      result = String::Builder.new
      pat_chars = pat.chars
      i = 0
      while i < pat_chars.size
        if pat_chars[i] == '['
          j = i + 1
          while j < pat_chars.size && pat_chars[j] != ']'
            j += 1
          end
          if j < pat_chars.size
            content = pat_chars[i + 1...j].join
            if !content.starts_with?('^') && content.size <= 2 && !content.empty?
              if content == "." || content == "\\."
                result << "\\."
              elsif content.size == 1
                result << content
              else
                result << content[1]
              end
            else
              result << '[' << content << ']'
            end
            i = j + 1
          else
            result << pat_chars[i]
            i += 1
          end
        else
          result << pat_chars[i]
          i += 1
        end
      end
      result.to_s
    end

    private def self.match_parts(parts : Array(String), str : String, anchored_start : Bool, anchored_end : Bool) : Bool
      return str.empty? if parts.empty?

      if parts.size == 1
        literal = parts[0]
        return str == literal if anchored_start && anchored_end
        return str.starts_with?(literal) if anchored_start
        return str.ends_with?(literal) if anchored_end
        return str.includes?(literal) || literal.empty?
      end

      pos = 0
      parts.each_with_index do |part, i|
        if i == 0
          if anchored_start
            return false unless str.starts_with?(part)
            pos = part.size
          else
            idx = str.index(part, pos)
            return false unless idx
            pos = idx + part.size
          end
        elsif i == parts.size - 1
          if anchored_end
            return str.ends_with?(part)
          else
            idx = str.index(part, pos)
            return false unless idx
            pos = idx + part.size
          end
        else
          idx = str.index(part, pos)
          return false unless idx
          pos = idx + part.size
        end
      end
      true
    end
  end
end
