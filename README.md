# Temporal Server review evidence

Focused evidence for selected Temporal Server PR reviews: test-only patches,
compact execution records, revision provenance, and reproduction instructions.
This directory contains no Temporal production source tree, build cache,
toolchain, or test binary.

| Review | Invariants | Evidence |
| --- | --- | --- |
| [PR #12772](12772/) | Pagination byte reservations survive merge and context cleanup until explicit owner release, and remain reserved during persistence before handler cleanup. | Two corrected BASE RED / HEAD GREEN comparisons, plus earlier focused HEAD normal and race-detector runs. |

The package follows the case-oriented organization of
[Ray review-evidence](https://github.com/hikaru-212/ray/tree/review-evidence),
using the requested flat `12772/` case directory. Results are bounded by the
specific revisions, tests, and environment documented in each case.

Verify all evidence files, including this index, from this repository root:

```bash
sha256sum -c 12772/SHA256SUMS
```

This local repository was initialized with an unborn `review-evidence` HEAD.
It has no commit or production-code ancestry. All prepared files are untracked.
Its only configured remote is `https://github.com/hikaru-212/temporal.git`.
No remote branch was created during preparation. The first manual commit will
create the local branch ref; a subsequent manual push is required to publish it.
