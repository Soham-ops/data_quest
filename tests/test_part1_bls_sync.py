from rearc_data_quest.jobs.part1_bls_sync import _normalize_s3_prefix, _parse_bls_listing


def test_parse_bls_listing_filters_queries_dirs_and_external_links():
    html = """
    <html><body>
      <a href="?C=N;O=D">Sort by name</a>
      <a href="../">Parent Directory</a>
      <a href="pr.data.0.Current">pr.data.0.Current</a>
      <a href="pr.data.1.AllData">pr.data.1.AllData</a>
      <a href="subdir/">subdir/</a>
      <a href="https://example.com/not-allowed.txt">external</a>
      <a href="pr.data.0.Current">duplicate</a>
    </body></html>
    """

    files = _parse_bls_listing(html, "https://download.bls.gov/pub/time.series/pr/")

    assert [f.name for f in files] == ["pr.data.0.Current", "pr.data.1.AllData"]
    assert files[0].url.endswith("/pr.data.0.Current")


def test_normalize_s3_prefix_adds_trailing_slash():
    assert _normalize_s3_prefix("raw/bls/pr") == "raw/bls/pr/"
    assert _normalize_s3_prefix("raw/bls/pr/") == "raw/bls/pr/"

