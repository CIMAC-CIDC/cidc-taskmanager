"""
Testing for hugo tasks
"""
from unittest.mock import patch
from framework.tasks.hugo_tasks import check_symbols_valid, build_gene_collection


def test_build_gene_collection():
    """
    Test build_gene_collection
    """
    tsv = (
        "#\n"
        + "9606	1	A1BG	-	A1B|ABG|GAB|HYST2477	"
        + "MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410|Vega:OTTHUMG00000183507	19	19q13.43	"
        + "alpha-1-B glycoprotein	protein-coding	A1BG	alpha-1-B glycoprotein	O	"
        + "alpha-1B-glycoprotein|HEL-S-163pA|epididymis secretory sperm binding protein Li 163pA	"
        + "20180805	-"
    )
    assert len(build_gene_collection(tsv)) == 5


# def test_check_symbol_valid():
#     """
#     test for check_symbol_valid
#     """
#     with patch(
#         "framework.tasks.AuthorizedTask.get_token", return_value={"access_token": "FOO"}
#     ):
#         with patch(
#             "framework.tasks.hugo_tasks.EVE.get",
#             return_value={
#                 "_items": [
#                     {"symbol": "FOO"},
#                     {"symbol": "BAR"},
#                     {"symbol": "ABC"},
#                     {"symbol": "DEF"},
#                 ]
#             },
#         ):
#             result = check_symbols_valid(["FOO", "BAR", "ABC", "DEF"])
#             assert not result
