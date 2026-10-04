# Pre-commit & Revive Behavior Reference

This document explains **exactly what happens** when you run `git add` and `git commit`
in this repository, based on the current `.pre-commit-config.yaml` and `revive.toml`.

Its purpose is **memory refresh and onboarding**, not configuration.

---

## 1. When do these checks run?

They run **automatically before every `git commit`**, because:

- `pre-commit` is installed
- `.git/hooks/pre-commit` exists
- required binaries (`gofumpt`, `revive`) are present

No manual command is required once installed.

---

## 2. High-level commit flow

When you run:

```bash
git add .
git commit -m "message"
```

Git executes **pre-commit hooks in order**:

1. Clean whitespace / EOF
2. Check merge conflict markers
3. Format Go code (`gofumpt`)
4. Lint Go code (`revive`)

If **any hook modifies files**, the commit is **aborted on purpose**.
You must re-stage and commit again.

This is expected behavior.

---

## 3. Hooks that MODIFY files (commit will fail the first time)

### trailing-whitespace
- Removes trailing spaces at end of lines
- Modifies files directly

### end-of-file-fixer
- Ensures files end with a newline
- Modifies files directly

### gofumpt
- Rewrites all Go files using `gofumpt` formatting rules
- Command used:
  ```bash
  gofumpt -l -w .
  ```
- Excludes:
  - `schemas/gen/`

**Result:**
- Files change → index != working tree → commit stops
- You must run:
  ```bash
  git add .
  git commit
  ```

This is normal and intentional.

---

## 4. Hooks that DO NOT modify files (lint only)

### check-merge-conflict
- Fails commit if conflict markers like `<<<<<<<` exist
- Does not modify files

### revive
- Static analysis only (no auto-fix)
- Command used:
  ```bash
  revive -config revive.toml -formatter stylish ./...
  ```
- Scans the **entire Go module**
- If severity = error → commit is blocked
- If severity = warning/info → commit allowed (unless policy changes)

---

## 5. Revive global behavior

### Generated code handling
```toml
ignoreGeneratedHeader = false
```

Meaning:
- Code marked as generated **is ignored**
- This is reverse logic and is intentional

### Default severity
```toml
severity = "info"
```

- Affects color / output style
- Does NOT disable rules
- Errors still block commits

---

## 6. Revive rules that BLOCK commits (severity = error)

These will **fail the commit** immediately:

- exported: exported identifiers must have comments
- redefines-builtin-id: cannot shadow built-in identifiers
- constant-logical-expr: forbids `if true`, `1 == 1`
- time-equal: forbids `t1 == t2` for `time.Time`
- comment-spacings: enforces strict comment formatting

---

## 7. Revive rules that WARN only

These will **not block commits**, but appear in output:

- var-naming
- confusing-naming
- max-public-structs (limit = 5)
- function-result-limit (limit = 3)
- indent-error-flow
- unused-receiver
- cyclomatic (limit = 15)
- deep-exit
- early-return
- empty-lines
- line-length-limit (120 chars)
- duplicated-imports
- import-shadowing

Warnings exist to guide refactoring, not to stop progress.

---

## 8. Why commits often succeed on the SECOND attempt

Because:
- First attempt: formatters modify files → commit aborted
- Second attempt: no changes needed → all hooks pass

This is **by design**, not a misconfiguration.

---

## 9. How to reduce failed first commits (optional workflow)

Recommended habit:

```bash
gofumpt -w .
git add .
git commit
```

This aligns your working tree with pre-commit expectations.

---

## 10. One-line summary

> pre-commit here is **format-first, lint-second**.
> A failed first commit usually means the system just fixed your code.

End of document.
