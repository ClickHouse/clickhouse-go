# Contributing notes

## Pull Request Guidelines

When creating a pull request from a fork, please **enable "Allow edits from maintainers"** in the PR sidebar. This allows maintainers to make minor fixes (typos, formatting, small adjustments) directly to your branch, which speeds up the review process and helps get your changes merged faster.

To enable it:
1. Open your pull request
2. Look at the right sidebar
3. Check the box "Allow edits and access to secrets by maintainers"

[Learn more](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork)

## Local setup

The easiest way to run tests is to use Docker Compose:

```bash
make up
make test
make down
```
