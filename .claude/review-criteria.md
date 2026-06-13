Report only issues you are confident about:
- Bugs and logic errors
- Security vulnerabilities
- Performance issues (especially allocations in hot paths)
- Missing error handling
- Crystal anti-patterns
- Missing specs (all code changes must have corresponding specs)

Quality bar:
- Only report issues you'd block a PR for. Skip stylistic nits and minor suggestions.
- If you start describing an issue and realize it's not actually a problem, drop it. Do not include findings you've talked yourself out of.
- Each finding must have a concrete file:line reference and a clear explanation of the actual impact.

If no problems found, say "No issues found."

Do NOT:
- Check CI status or previous commits
- Run specs
- Use emojis
- Explore the codebase broadly — only read specific files when needed
