// This file has a sensitive filename. It MUST be loaded after version.sbt, so it can rely in sbt-release version
// Even while the name seems a typo, that "ensures" that it will be loaded as expected.

version in ThisBuild := VersionWithSHA.kamonVersionWithSHA((version in ThisBuild).value)

isSnapshot in ThisBuild := VersionWithSHA.kamonIsSnapshot((version in ThisBuild).value)
