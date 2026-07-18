#!/bin/sh

set -eu

modfile=${1:-}
if [ -z "$modfile" ] || [ ! -f "$modfile" ]; then
	echo "release modfile is not prepared: ${modfile:-go.release.mod}" >&2
	exit 1
fi

awk '
function trim(value) {
	gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
	return value
}

function reject_local(replacement, target, fields) {
	replacement = trim(replacement)
	sub(/[[:space:]]+\/\/.*$/, "", replacement)
	replacement = trim(replacement)

	fields = split(replacement, parts, /[[:space:]]+/)
	target = parts[1]
	gsub(/^"|"$/, "", target)

	# A replacement target without a version is a filesystem directory under
	# the Go module grammar, even when it does not begin with dot or slash.
	if (fields < 2 || target ~ /^\.\.?\// || target ~ /^\// ||
	    target ~ /^file:/ || target ~ /^~/ || target ~ /^[A-Za-z]:[\\\/]/ ||
	    target ~ /^\\\\/) {
		printf "release modfile contains local filesystem replacement: %s\n", replacement > "/dev/stderr"
		failed = 1
		exit 1
	}
}

BEGIN {
	in_replace_block = 0
	failed = 0
}

{
	line = trim($0)
	if (line == "" || line ~ /^\/\//) {
		next
	}

	if (line ~ /^replace[[:space:]]*\($/) {
		in_replace_block = 1
		next
	}
	if (in_replace_block && line == ")") {
		in_replace_block = 0
		next
	}

	if (!in_replace_block) {
		if (line !~ /^replace[[:space:]]+/) {
			next
		}
		sub(/^replace[[:space:]]+/, "", line)
	}

	separator = index(line, "=>")
	if (separator == 0) {
		printf "malformed replace directive in release modfile: %s\n", line > "/dev/stderr"
		failed = 1
		exit 1
	}
	reject_local(substr(line, separator + 2))
}

END {
	if (!failed && in_replace_block) {
		print "unterminated replace block in release modfile" > "/dev/stderr"
		exit 1
	}
}
' "$modfile"
