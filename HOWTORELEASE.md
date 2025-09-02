# How to release

What's a release? For now, a release is a non-SNAPSHOT version with
corresponding vX.Y.Z tag in the repository, and a corresponding GitHub
Releases page.

Later on, we'll work on signing the tag and uploading to Maven Central.

The examples in this document assume that the current development
version is 0.3.1-SNAPSHOT, that we're releasing version 0.3.1, and that
the next development version is 0.3.2-SNAPSHOT.

Quick checklist

- [ ] bring CHANGELOG.md up to date
- [ ] ./changelog-tool.py --rename=0.3.1
- [ ] run ./mvnw release:prepare
- [ ] push the new tag and the new commits to GitHub,  a GitHub Action
      will then build it and create a GitHub Release page


## CHANGELOG

In the file [CHANGELOG.md](CHANGELOG.md) we keep track of the notable
changes per released version. It contains one section per released
version, ordered newest to oldest.

The CHANGELOG must be a readable list of changes, not a dumping ground
for cryptic commit messages.

The top section is always `# Unreleased`. It describes the as-yet
unreleased version. The first step is to thoroughly check this for
completeness and readability, and generally tidy things up.

NOTE: it's allowed to tidy up the wording of earlier versions, too.

Just before releasing, the Unreleased section must be renamed and a new
Unreleased section must be placed above it. The script
`changelog-tool.py` can help, run

```shell
./changelog-tool.py --rename=0.3.1
```

Then commit this change.


## Bumping version numbers and creating a tag

In monetdb-spark/, run `./mvnw release:prepare`. This will

- Prompt for the new version number (0.3.1)

- Insert it in pom.xml without a -SNAPSHOT suffix.

- Run the tests

- Commit the changed pom.xml

- Tag this commit

- Prompt for the new development version (0.3.2-SNAPSHOT)

- Change pom.xml to the new development version

- Commit this change

You should now manually verify these changes one more time and then push
the commits and the tag to the repository.


## Creating a new release on GitHub

Should happen automatically.

Whenever a tag of the form vX.Y.Z is pushed to GitHub, the
[create-release.yml](.github/workflows/create-release.yml) workflow
is triggered. This will build the jar file and create a Release
page for it.
