# Repo Standards

These rules apply to all GrantKlassy repos. Enforced by lefthook hooks and `task check`.

## Commits

- **Never add `Co-Authored-By` trailers to commit messages.** Not as a suggestion, not as a default, not ever. Do not attempt it. Lefthook hooks reject them, but do not rely on the hooks — simply never write the trailer. GitHub counts Co-Authored-By as contributors and it misrepresents authorship.
- All commits must be authored as `GrantKlassy`. The git identity is managed by `~/.gitconfig` includeIf — do not set it locally.
- Push via the `github.com-gk` SSH alias only.
