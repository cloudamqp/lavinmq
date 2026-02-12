#!/bin/sh
# Processes SHTML (SSI) directives and outputs HTML.
# Usage: views/render.sh views/page.shtml "1.2.3" > static/page.html
#
# Supported directives:
#   <!--#set var="NAME" value="..." -->   Store a variable
#   <!--#include file="path" -->          Include file (relative to views/)
#   <!--#echo var="NAME" -->              Output variable value
set -eu

VIEWS_DIR="$(cd "$(dirname "$0")" && pwd)"
INPUT="$1"
VERSION="${2:-dev}"

process() {
  local line rest out before directive tag after varname value file
  while IFS= read -r line; do
    # Process all directives that may appear on a line
    rest="$line"
    out=""
    while :; do
      case "$rest" in
        *'<!--#'*)
          # Text before the directive
          before="${rest%%<!--#*}"
          out="$out$before"
          directive="${rest#*<!--#}"
          # Extract the full directive up to -->
          tag="${directive%%-->*}"
          after="${directive#*-->}"
          # Remove trailing space from tag
          tag="${tag% }"
          case "$tag" in
            set\ var=\"*\"\ value=\"*\")
              varname="${tag#set var=\"}"
              varname="${varname%%\"*}"
              value="${tag#*value=\"}"
              value="${value%\"}"
              eval "VAR_$varname=\"\$value\""
              ;;
            include\ file=\"*\")
              file="${tag#include file=\"}"
              file="${file%\"}"
              process < "$VIEWS_DIR/$file"
              ;;
            echo\ var=\"*\")
              varname="${tag#echo var=\"}"
              varname="${varname%\"}"
              eval "out=\"\${out}\${VAR_$varname}\""
              ;;
          esac
          rest="$after"
          ;;
        *)
          out="$out$rest"
          break
          ;;
      esac
    done
    printf '%s\n' "$out"
  done
}

VAR_VERSION="$VERSION"
process < "$INPUT"
