import argparse
import re
import subprocess
import textwrap
from typing import Tuple, Optional

__all__ = ['get_version_string_from_git']


def get_version(cwd: Optional[str] = None) -> Tuple[str, ...]:
    """
    Return: a tuple containing last branch tag (possibly empty),
            last release tag, and possibly a "dirty" string

    This script has to be run within the Git repository.
    it will inspect the state of the current tree/branch in the workspace.
    Functionally equivalent to the Perl script "version", and called only from setup.py

    Version type is used to select the appropriate grep pattern.
    The idea is to look for either the latest master/release type tag "\d+.\d+.\d+.\d+"
    or PIPE-<number>-x tags. Both are required to construct a meaningful number for
    the pipeline wheels.

    The output of get_version() is a tuple of zero, two or three elements:
     * 0-element tuple: if the git history is not found
     * 2/3-element tuple: 
          * last reachable branch tag (possibly empty string)
          * last reachable release tag
          * optionally: a "dirty" suffix.
    e.g:
      ('2024.0.0.3','2024.0.0.3')
      or
      ()'','2024.0.0.3','dirty')
    Note that: the dirty state definition is slightly different from the traditional Git convention:
      'dirty' is given if any of those conditions are met:
          * the latest reachable branch tag is empty
          * the latest tag is not the latest commit
          * the dirty repo
    """

    def tonumber(value):
        try:
            return int(value)
        except ValueError:
            return 0

    def get_last_tag(gitbranch, branchpattern, delim):
        branchpattern = re.compile(branchpattern)
        delim = re.compile(delim)
        releaseid = gitbranch.split('/')[-1] if 'release/' in gitbranch else ''
        versions = {}
        for githash in hashes:
            for line in refstags:
                # if 'release/' is not in gitbranch, the second condition is always true
                if line.startswith(githash) and releaseid in line:
                    tag = line.replace(githash+' refs/tags/', '').replace('^{}', '')
                    if branchpattern.match(tag):
                        # split the version string into a tuple by the given delimiter,
                        # then convert each element of the tuple from string to int
                        # in order to sort the versions correctly.
                        # "versions" is a dict in which keys are the parsed tuples of ints,
                        # and values are the original version strings
                        versions[tuple(tonumber(x) for x in delim.split(tag))] = tag
        # sort the dict items by the first element (tuples of ints)
        # and return the second element (the corresponding version string)
        # of the highest value (i.e. most recent version)
        return sorted(versions.items())[-1][1] if versions else ''

    def get_last_branch_tag(gitbranch):
        match = re.match(r'(PIPE-\d+)', gitbranch)
        if match:
            branchpattern = match.group(1) + r'-\d+'
        else:
            branchpattern = gitbranch + r'-\d+'
        return get_last_tag(gitbranch, branchpattern, '-')

    def get_last_release_tag(gitbranch):
        return get_last_tag(gitbranch, r'^\d+\.\d+\.\d+\.\d+$', r'\.')

    try:
        gitbranch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        # if cwd is not within a Git repo, a CalledProcessError Exception
        # will be triggered due to a non-zero subprocess exit status and no need
        # to continue.
        return ()

    gitbranch = subprocess.check_output(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()

    hashes = subprocess.check_output(
        ['git', 'log', '--since', '2019-10-01', '--simplify-by-decoration', "--pretty=%H", gitbranch],
        stderr=subprocess.DEVNULL, cwd=cwd).decode().splitlines()

    refstags = subprocess.check_output(
        ['git', 'show-ref', '--tags', '-d'],
        stderr=subprocess.DEVNULL, cwd=cwd).decode().splitlines()

    last_release_tag = get_last_release_tag(gitbranch)
    if gitbranch in ['main', 'master'] or 'release/' in gitbranch:
        last_branch_tag = last_release_tag
    else:
        last_branch_tag = get_last_branch_tag(gitbranch)

    output = (last_branch_tag, last_release_tag)
    dirty_workspace = subprocess.call(['git', 'diff', '--quiet'], cwd=cwd)

    if last_branch_tag == '' or dirty_workspace:
        # No tag at all for branch
        output += ('dirty',)
    else:
        # Check if the latest tag is the latest commit
        headcommit = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()
        tagcommit = subprocess.check_output(
            ['git', 'rev-list', '-n', '1', last_branch_tag],
            stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()
        if tagcommit != headcommit:
            output += ('dirty',)

    return output


def get_version_string_from_git(cwd: Optional[str] = None, verbose: bool = False) -> str:
    """Retrieves the version of the package based on Git repository information.

    Args:
        cwd (str, optional): Current working directory. Defaults to None.
        verbose (bool, optional): If True, prints verbose output. Defaults to False.

    Returns:
        str: The version of the package, formatted as 'public_label+local_label', compatible with PEP440.

    The version consists of a 'public_label' and an optional 'local_label' separated by a '+'. The 'public_label'
    represents the most recent release tag, while the 'local_label' includes information about the current branch,
    number of commits since the last tag, the commit hash abbreviation, etc.

    If the current directory is not inside a Git repository or Git commands fail, the function returns 'unknown'.
    examples:
        $ python version.py -f
        $ 2023.0.0.32+srdp.2023.1b1-48-g198ca2972-dirty-PIPE-1447-implement-self-calibration-task-hif_selfcal    
    To get the inline help, try: 
        $ python version.py --help

    """

    try:
        # Silently test if CWD is inside a Git repo; if not, a CalledProcessError Exception
        # will be triggered due to a non-zero subprocess exit status.
        subprocess.check_output(['git', 'rev-parse'], cwd=cwd, stderr=subprocess.DEVNULL)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return 'unknown'

    # retrieve the info about the current branch
    try:
        git_branch = subprocess.check_output(['git', 'symbolic-ref', '--short', 'HEAD'],
                                             stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        # https://stackoverflow.com/a/52222248
        # A detached HEAD pointing at a specific tag/commit instead of a branch top will cause a
        # non-zero exit status; in that case, we set git_branch to None.
        git_branch = 'detached'

    # try to get tag information, but we ignore the nontraditional 'dirty' check from get_version()
    tag_tuple = get_version(cwd=cwd)
    last_branch_tag = 'unknown_recent_branch_tag'
    last_release_tag = 'unknown_recent_release_tag'
    if len(tag_tuple) >= 2:
        last_branch_tag = tag_tuple[0]
        last_release_tag = tag_tuple[1]
    if verbose:
        print('# output generated by version.get_version(), backward compatible to the pre-PIPE-1939 Perl script:')
        print(tag_tuple)
        print('')

    # Retrieve info about current commit.
    #
    # The git-describe call output format, e.g. 2024.0.0.22-27-gc9ac2824d
    # which consists of three elements jointed by '-':
    #       * most recent lightweight tag;
    #       * number of comments since that tag
    #       * commit hash abbreviation with a 'g' prefix
    # note:
    #   '--tags': search lightweight (non-annotated) tags rather than the default annotated tags
    #   '--dirty': this flag optionally adds the '-dirty' string if the repo state is dirty
    cmd = ['git', 'describe', '--always', '--tags', '--long', '--dirty']
    describe_output = subprocess.check_output(cmd,
                                              stderr=subprocess.DEVNULL, cwd=cwd).decode().strip()
    if verbose:
        print('# output from "{}":'.format(' '.join(cmd)))
        print(describe_output)
        print('')
    if describe_output.endswith('-dirty'):
        describe_output = describe_output[:-6]
        dirty = True
    else:
        dirty = False
    recent_tag, num_commit, hash_abbr = describe_output.rsplit("-", 2)

    # In the Pipeline development, only main branch tags are guaranteed to meet PEP440, so we use that for the public label.
    public_label = last_release_tag

    is_release_or_main = isinstance(git_branch, str) and git_branch != 'main' and not git_branch.startswith('release/')

    local_label = []
    if recent_tag != last_release_tag:
        local_label.append(recent_tag)
    if num_commit != '0':
        local_label.append(f'{num_commit}')
    if dirty or num_commit != '0' or (not is_release_or_main) or git_branch is None:
        local_label.append(f'{hash_abbr}')
    if dirty:
        local_label.append('dirty')
    if is_release_or_main:
        local_label.append(f'{git_branch}')
    local_label = '-'.join(local_label)

    # PIPE-2068: Consolidate into single version string.
    # Only ASCII numbers, letters, '.', '-', and '_' are allowed in the local version label
    local_label = re.sub(r'[^\w_\-\.]+', '.', local_label)
    version = public_label
    if local_label:
        version += f'+{local_label}'

    return version


if __name__ == '__main__':

    desc = """
    This script allows you to derive the optimal package string compatible with PEP440.
    When running inside a Git Repo and it will print the results out.
    
    For backward compatibility with the old pre-PIPE-1913 Perl script, the default output is a string consistent 
    with two or three elements joint by an empty space string ' ', which is not PEP440-compatibel, and notsuitable
    for package versioning. We only use it for for backward compatibility with the online CI/CD service.
    
    The --full-string mode is the one used by the Pipeline development and installation.
    This script alone can be used as a command-line tool outside of the NRAO/ALMA/NAOJ Pipeline development as a lightweight
    replacement of setuptools-scm.

    Some examples:
        
        $ python version.py # note the leading space.
        $  2024.0.0.28 dirty
        
        $ python version.py --full-string # used by the Pipeline development.
        $ 2024.0.0.28+29-gc9ac2824d-dirty-PIPE-1669-run-dev-pipeline-with-modular-casa6
        
        $ python -m setuptools_scm # as a comparison
        $ 2024.0.0.29.dev29+gc9ac2824d.d20240318
    
    As a comparison, the pre-PIPE-1669 version string value is:
        2024.0.0.28+2024.0.0.28-29-gc9ac2824d-dirty-PIPE-1669-run-dev-pipeline-with-modular-casa6
    """
    parser = argparse.ArgumentParser(description=textwrap.dedent(desc),
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--full-string', dest="full_string", action="store_true",
                        help='Generate the full desired Pipeline version string.')
    parser.add_argument('-v', '--verbose', dest="verbose", action="store_true",
                        help='Print out additional debugging statement.')
    args = parser.parse_args()
    if args.full_string:
        print(get_version_string_from_git(verbose=args.verbose))
    else:
        # For the online CI/CD legacy compatibility.
        tag_tuple = get_version()
        if len(tag_tuple) > 1:
            print(' '.join(tag_tuple))
        else:
            print('unknown')
