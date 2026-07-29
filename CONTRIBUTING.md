# Contributing

## Commit messages

This project follows [Conventional Commits](https://www.conventionalcommits.org/): the release changelog is generated
automatically from commit messages since the last tag, grouped by type.

Format:

```
<type>(<optional scope>): <subject>

<optional body>
```

Common types:

| Type       | Use for                                            |
|------------|-----------------------------------------------------|
| `feat`     | A new feature                                        |
| `fix`      | A bug fix                                            |
| `chore`    | Maintenance work with no user-facing effect           |
| `docs`     | Documentation only                                    |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf`     | Performance improvement                               |
| `test`     | Adding or fixing tests                                |
| `build`    | Build system or dependency changes                    |
| `ci`       | CI configuration changes                              |

Examples:

```
feat(cli): add --create-target-database option
fix(logging): route driver logs through logback instead of raw JUL
```

Commits that don't follow this format still work fine (nothing breaks, no commit hook enforces this) — they just land
in an "Uncategorized" section of the generated changelog instead of a proper category, so following the convention is
worth it for anything user-facing.

## Releasing

A release is triggered by pushing a tag matching `v*.*.*` to `main`:

```bash
git tag v1.2.0
git push origin v1.2.0
```

`.github/workflows/release.yml` then:

1. Extracts the version from the tag (strips the leading `v`).
2. Runs `mvn clean verify -Drevision=<version>` — full build and tests.
3. Runs `mvn jreleaser:full-release -Drevision=<version>`, which:
   - Generates a changelog from commits since the previous tag (see the Commit messages section above).
   - Creates a GitHub Release named `Release <version>`, tagged `v<version>`.
   - Attaches the shaded jar and the really-executable binary (`neo4j-db-copy`).

No local build or manual upload needed, and no manual version bump in `pom.xml` — `revision` is injected at build
time via `-Drevision`.
