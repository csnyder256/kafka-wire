package main

import "flag"

// permute reorders arguments so flags may appear anywhere on the command
// line, not only before the first positional argument.
//
// Go's flag package stops parsing at the first non-flag argument, so
//
//	kafka-wire topic create demo --brokers host:9092
//
// silently ignores --brokers and connects to the default instead. That is a
// wrong answer delivered confidently, which is worse than an error, and it is
// exactly the shape of the command a user will type. Sorting the flags to the
// front before parsing makes both orders work.
//
// A flag that takes a value consumes the argument after it unless it was
// written as --name=value or is boolean.
func permute(fs *flag.FlagSet, args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if len(a) < 2 || a[0] != '-' {
			positional = append(positional, a)
			continue
		}
		flags = append(flags, a)

		// --name=value carries its own value.
		hasInlineValue := false
		for j := 1; j < len(a); j++ {
			if a[j] == '=' {
				hasInlineValue = true
				break
			}
		}
		if hasInlineValue {
			continue
		}
		if i+1 < len(args) && takesValue(fs, a) {
			i++
			flags = append(flags, args[i])
		}
	}
	return append(flags, positional...)
}

// takesValue reports whether a flag consumes the following argument. Boolean
// flags do not, which is what makes "-from-beginning demo" work.
func takesValue(fs *flag.FlagSet, arg string) bool {
	name := arg
	for len(name) > 0 && name[0] == '-' {
		name = name[1:]
	}
	f := fs.Lookup(name)
	if f == nil {
		// Unknown flag. Let flag.Parse produce the error message rather than
		// guessing and swallowing the next argument.
		return false
	}
	if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
		return false
	}
	return true
}
