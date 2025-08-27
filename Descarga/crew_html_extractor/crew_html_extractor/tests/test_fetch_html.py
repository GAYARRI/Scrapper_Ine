from crew_html_extractor.tools.fetch_html import FetchHTMLTool

def test_fetch_html_example_org():
    html = FetchHTMLTool()._run("https://example.org")
    assert "<html" in html.lower()
    assert "example" in html.lower()
