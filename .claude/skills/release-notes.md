# Release Notes Generator

You are helping create release notes for a new version. Follow these guidelines to produce high-quality, accurate release notes.

## Methodology

### 1. Determine Version Scope

First, identify:
- The new version being released (e.g., 2.5.0)
- The previous stable version (e.g., 2.4.5)
- Any pre-release versions (beta, RC) that came before

**Important**: Only include changes that are new compared to the previous stable release. Do not include:
- Bugs introduced and fixed within pre-release versions
- Changes already backported to previous stable versions

### 2. Gather Changes

Use git to understand what's included:

```bash
# List tags to identify versions
git tag --sort=-version:refname | head -20

# See commits between versions
git log --oneline --no-merges v<previous>..v<new> | head -50

# Check the CHANGELOG for organized view
# Read CHANGELOG.md focusing on the target version section
```

### 3. Verify Each Reference

For every PR or commit you mention in release notes, verify it's actually new:

```bash
# Check if a PR is in the previous stable branch
git log v<previous-stable>..<latest-stable> --oneline --grep="#<PR_NUMBER>"

# Check which versions contain a commit
git tag --contains <commit-hash> | grep "^v<major>.<minor>"
```

If a change appears in the previous stable version, **do not include it**.

### 4. Organize Content

Structure the release notes with these sections:

1. **Breaking Changes** - Things that require user action or awareness
   - Deprecated features (with timeline for removal)
   - Changed requirements (dependencies, versions)
   - Behavioral changes that could affect users

2. **Performance Improvements** - Measurable performance gains
   - Boot time optimizations
   - Memory usage improvements
   - Network/I/O optimizations
   - Clustering improvements

3. **User Interface** - Visual and UX changes
   - New themes or visual updates
   - Navigation improvements/changes
   - New features in the management interface

4. **Feature Area Sections** (e.g., Clustering, Streams, MQTT, Tooling, API)
   - Group by functional area
   - Include new capabilities and improvements

5. **Bug Fixes** - Important fixes
   - Group by area (routing, clustering, authentication, etc.)
   - Focus on user-visible bugs
   - Skip internal/development-only fixes

6. **Upgrade Notes** - Practical upgrade guidance
   - Before upgrading (backup, preparation)
   - Upgrade considerations (things that happen automatically but users should know)
   - During upgrade (what to monitor)
   - After upgrading (verification steps)

### 5. Writing Style

**Tone**: Humble, professional, casual, concise

**Do**:
- Write in flowing text, not bullet lists (except for lists like deprecated metrics or steps)
- Include PR/commit references inline: `[#1234](https://github.com/org/repo/pull/1234)`
- Explain what changed and why it matters
- Use active voice
- Be specific and factual
- Keep sentences short and direct

**Don't**:
- Use marketing language or superlatives ("dramatically", "revolutionary", "game-changing")
- Make boastful claims ("the fastest", "the best")
- Over-elaborate or add unnecessary details
- Use excessive adjectives ("significant", "substantial", "tremendous")
- Write a "Looking Forward" or "Conclusion" section with future promises

**Good example**:
```
Boot time has been optimized by storing message counts in metadata files [#1163].
Previously, LavinMQ scanned all message segments on startup to count messages.
With metadata files, servers with large message backlogs can now start up much faster.
```

**Bad example**:
```
One of the most impactful and revolutionary changes in 2.5.0 is our dramatic
optimization of server boot time [#1163]. This game-changing improvement delivers
tremendous value and represents a significant milestone in LavinMQ's evolution.
```

### 6. Breaking Changes Guidance

Only include true breaking changes:
- Features being removed or deprecated
- Changed system requirements
- Behavioral changes that could break existing setups

**Do not include** as breaking changes:
- Internal file format changes that migrate automatically
- Fixes that correct wrong behavior (even if users relied on the bug)
- Changes that are backwards compatible

### 7. Reference Format

When mentioning changes, include references:
- PRs: `[#1234](https://github.com/owner/repo/pull/1234)`
- Commits: `[abc1234](https://github.com/owner/repo/commit/abc1234567890)`

Include a reference when you describe a feature or fix, but you don't need to list every related commit. One or two main PR references per feature is sufficient.

## Process

1. **Identify versions**: Ask user which version is being released and what the previous stable was
2. **Read CHANGELOG**: Check if there's a CHANGELOG.md with initial content
3. **Verify scope**: Use git to verify what's new since the previous stable
4. **Create todo list**: Use TodoWrite to track sections you'll write
5. **Write sections**: Go through each major area
6. **Verify references**: Double-check all PR/commit references are correct
7. **Review tone**: Ensure writing is humble and concise

## Example Workflow

```
User: "Create release notes for 2.5.0"

1. Check git tags to find:
   - Previous stable: v2.4.5
   - New version: v2.5.0
   - Pre-releases: v2.5.0-beta.1, v2.5.0-rc.1 through rc.7

2. Check CHANGELOG.md for v2.5.0 section

3. Verify what's new: git log v2.4.5..v2.5.0-beta.1

4. For each PR in CHANGELOG:
   - Verify it's not in v2.4.0..v2.4.5
   - If it is in 2.4.x, exclude it

5. Organize into sections

6. Write in flowing, concise text with inline references

7. Review for boastful language and remove it
```

## Questions to Ask User

- What version are you releasing?
- What was the previous stable version?
- Should I exclude any particular changes?
- Are there any breaking changes I should highlight?
- Do you have a specific tone preference?

## Common Mistakes to Avoid

1. Including changes that were backported to previous stable versions
2. Using marketing language instead of technical descriptions
3. Writing too much explanation for simple changes
4. Forgetting to include PR/commit references
5. Listing breaking changes that aren't actually breaking (auto-migrations)
6. Writing long paragraphs instead of concise descriptions
7. Using the CHANGELOG as-is instead of writing flowing text

## Output Format

Deliver as a Markdown file with:
- Clear section headers
- Inline links to PRs/commits
- Concise, flowing text
- Code formatting for technical terms
- Organized by theme, not by PR number

The release notes should be readable by both technical and non-technical users, focusing on what changed and why it matters to them.
