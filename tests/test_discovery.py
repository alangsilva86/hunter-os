import unittest

from modules import enrichment_async as ea


class DiscoveryTests(unittest.TestCase):
    def test_excluded_domain(self) -> None:
        self.assertTrue(ea._is_excluded_domain("econodata.com.br"))
        self.assertFalse(ea._is_excluded_domain("acme.com.br"))

    def test_parked_domain(self) -> None:
        html = "This domain is for sale - GoDaddy"
        self.assertTrue(ea._is_parked_domain(html, {}))
        self.assertTrue(ea._is_parked_domain("", {"server": "godaddy"}))

    def test_candidate_scoring_brand_match(self) -> None:
        lead = {
            "nome_fantasia": "Acme Engenharia",
            "razao_social": "Acme Engenharia Ltda",
            "municipio": "Maringa",
            "uf": "PR",
        }
        candidate = {
            "url": "https://acme.com.br",
            "domain": "acme.com.br",
            "title": "Acme Engenharia",
        }
        html = (
            "<html><head><title>Acme Engenharia</title></head>"
            "<body>Contato Maringa PR <div>schema.org/Organization</div></body></html>"
        )
        score, reasons = ea.score_website_candidate(candidate, lead, html, {}, "https://acme.com.br")
        self.assertGreaterEqual(score, 60)
        self.assertIn("brand_match", reasons)


if __name__ == "__main__":
    unittest.main()
