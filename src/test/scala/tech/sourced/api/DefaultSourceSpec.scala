package tech.sourced.api

import org.scalatest._

class DefaultSourceSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var api: SparkAPI = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    api = SparkAPI(ss, resourcePath)
  }

  // TODO race condition sometimes
  "Default source" should "get heads of all repositories and count the files" in {
    val out = api.getRepositories.getHEAD.getCommits.getFirstReferenceCommit.getFiles.count()
    out should be(459)
  }

  it should "count all the commit messages from all masters that are not forks" in {
    val commits = api.getRepositories.filter("is_fork = false").getMaster.getCommits
    val out = commits.select("message").filter(commits("message").startsWith("a")).count()
    out should be(7)
  }

  it should "count all commits messages from all references that are not forks" in {
    val commits = api.getRepositories.filter("is_fork = false").getReferences.getCommits
    val out = commits.select("message").filter(commits("message").startsWith("a")).count()
    out should be(98)
  }

  it should "get all files from HEADS that are Ruby" in {
    val files = api.getRepositories.filter("is_fork = false")
      .getHEAD
      .getCommits.getFirstReferenceCommit
      .getFiles.classifyLanguages
    val out = files.filter(files("lang") === "Ruby").count()
    out should be(169)
  }

  it should "not optimize if the conditions on the " +
    "join are not the expected ones" in {
    val repos = api.getRepositories
    val references = ss.read.format("tech.sourced.api").option("table", "references").load()
    val out = repos.join(references,
      (references("repository_id") === repos("id"))
        .and(references("name").startsWith("refs/pull"))
    ).count()

    out should be(37)
  }

  // TODO no results
  it should "get the first commit of any reference that his content start" +
    " with 'import'" in {
    val out = api.getRepositories.getReferences.getCommits.getFirstReferenceCommit.getFiles.show()
  }

  "Convenience for getting files" should "work without reading commits" in {
    val spark = ss
    import spark.implicits._

    val filesDf = api
      .getRepositories.filter($"id" === "github.com/mawag/faq-xiyoulinux")
      .getReferences.getHEAD
      .getFiles
      .select(
        "repository_id",
        "reference_name",
        "path",
        "commit_hash",
        "file_hash",
        "content",
        "is_binary"
      )

    val cnt = filesDf.count()
    info(s"Total $cnt rows")
    cnt should be(2)

    info("UAST for files:\n")
    val filesCols = filesDf.columns.length
    val uasts = filesDf.classifyLanguages.extractUASTs

    val uastsCols = uasts.columns.length
    assert(uastsCols - 2 == filesCols)
  }

  "Filter by reference from repos dataframe" should "work" in {
    val spark = ss

    val count = api
      .getRepositories
      .getReference("refs/heads/develop")
      .count()

    assert(count == 2)
  }

  "Filter by HEAD reference" should "return only HEAD references" in {
    val spark = ss
    val count = api.getRepositories.getHEAD
      .select("name").distinct().count()

    assert(count == 1)
  }

  "Filter by master reference" should "return only master references" in {
    val spark = ss
    val df = api.getRepositories.getMaster

    assert(df.count == 5)
  }

  "Get develop commits" should "return only develop commits" in {
    val spark = ss
    val df = api.getRepositories
      .getReference("refs/heads/develop").getCommits

    assert(df.count == 103)
  }

  "Get files of master" should "return the files" in {
    val spark = ss
    val df = api.getRepositories.getMaster.getCommits
      .getFirstReferenceCommit.getFiles

    df.count() should be(459)
  }

  // TODO no results
  "Get files after reading commits" should "return the correct files" in {
    val files = api.getRepositories.getReferences.getCommits.getFiles

    assert(files.count == 1536360)
  }

  // TODO no results and wrong result (it must be the same result as the previous test,
  // or not be possible to execute this query)
  "Get files without reading commits" should "return the correct files" ignore {
    val files = api.getRepositories.getReferences.getFiles

    assert(files.count == 19126)
  }

  "Get files" should "return the correct files" in {
    val df = api.getRepositories.getHEAD.getCommits
      .sort("hash").limit(10)
    val rows = df.collect()
      .map(row => (row.getString(row.fieldIndex("repository_id")),
        row.getString(row.fieldIndex("hash"))))
    val repositories = rows.map(_._1)
    val hashes = rows.map(_._2)

    val files = api
      .getFiles(repositories.distinct, List("refs/heads/HEAD"), hashes.distinct)

    // TODO different result
    assert(files.count == 655)
  }

  // TODO wrong amount of files
  it should "return the correct files if we filter by repository" in {
    val files = api
      .getFiles(repositoryIds = List("github.com/xiyou-linuxer/faq-xiyoulinux"))

    assert(files.count == 20048)
  }

  it should "return the correct files if we filter by reference" in {
    val files = api
      .getFiles(referenceNames = List("refs/heads/develop"))

    assert(files.count == 425)
  }

  // TODO no results
  it should "return the correct files if we filter by commit" in {
    val files = api
      .getFiles(commitHashes = List("fff7062de8474d10a67d417ccea87ba6f58ca81d"))

    assert(files.count == 86)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    api = _: SparkAPI
  }
}
