# Pipeline Versioning

The Pipeline versioning following the principle convention outlined in [PEP440](https://peps.python.org/pep-0440/): public_label[+local_label]
The public label is the latest main branch tag that is reachable from the Git repo HEAD: in our current tagging scheme, the lightweight tag value always meets the [PEP440](https://peps.python.org/pep-0440/) requirements.
On the other hand, the "local_label" string is joined by several mandate or optical string elements with '-'. The format is inspired by the output of `git describe --long --tags --dirty --always` and various schemes used by [`setuptools-scm`](https://github.com/pypa/setuptools_scm), but gets further expanded with additional branching information:

* the latest branch tag that is reachable from the HEAD, if it is identical to b, it will be skipped
* the number of additional commits from the latest branch tag to the current HEAD
* abbreviated commit name (always with a 'g' prefix); this is skipped if the following conditions are all met: the repo state is clean; no additional commits from the recent branch tag to the HEAD; the branch is a "release" branches, or unknown (e.g. a HEAD detached state)
* the 'dirty' string : only included if the repo is in a "dirty" state. note that a detached HEAD is not considered "dirty" here.
* the branch name; however, if the HEAD is a release branch (`release/*` or `main`), it will be skipped.
